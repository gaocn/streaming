package org.apache.spark.streaming.receiver

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

import com.google.common.base.Throwables
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rpc.{RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.domain.ReceivedBlockInfo
import org.apache.spark.streaming.receiver.handler.{BlockManagerBasedBlockHandler, ReceivedBlockHandler, WriteAheadLogBasedBlockHandler}
import org.apache.spark.streaming.receiver.input.{ArrayBufferBlock, ByteBufferBlock, IteratorBlock, ReceivedBlock}
import org.apache.spark.streaming.wal.WriteAheadLogUtils
import org.apache.spark.util.RpcUtils
import org.apache.spark.{Logging, SparkEnv}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
	* 负责存储接收器接收到的数据，接收器将接收到的数据给supervisor，由
	* supervisor存储成功后，将元数据信息发送给Driver中的ReceiverTracker
	*/
private[streaming] class ReceiverSupervisorImpl(
															receiver: Receiver[_],
															env: SparkEnv,
															hadoopConf: Configuration,
															checkpointDirOption: Option[String])
	extends ReceiverSupervisor(receiver, env.conf) with Logging {

	private val host = SparkEnv.get.blockManager.blockManagerId.host
	private val executorId = SparkEnv.get.blockManager.blockManagerId.executorId

	/**
		* 通过ReceivedBlockHandler写数据的两种方式：
		* 	1、WAL；2、使用BlockManager直接存储
		*/
	private val receivedBlockHandler: ReceivedBlockHandler = {
		if(WriteAheadLogUtils.enableReceiverLog(env.conf)) {
			if (checkpointDirOption.isEmpty) {
				throw new Exception("没有设置checkpoint dir无法启用WAL" +
					"功能，可以通过SContext.checkpoint进行配置")
			}
			/** 基于WAL数据容错，因为接收器可能很多因此需要receiver做标识 */
			new WriteAheadLogBasedBlockHandler(env.blockManager,receiver.streamId,
				receiver.storageLevel, env.conf, hadoopConf,checkpointDirOption.get)
		} else {
			/** 基于数据备份的容错，底层基于RDD容错 */
			new BlockManagerBasedBlockHandler(env.blockManager, receiver.storageLevel)
		}
	}

	/** ReceiverTracker远程通信实体 */
	private val trackerEndpoint = RpcUtils.makeDriverRef("ReceiverTtracker", env.conf, env.rpcEnv)

	/** 接收来自Driver中ReceiverTracker消息的通信实体*/
	private val endpoint = env.rpcEnv.setupEndpoint(
		s"Receiver-${receiver.streamId}-${System.currentTimeMillis()}",
		new ThreadSafeRpcEndpoint {
			override val rpcEnv: RpcEnv = env.rpcEnv

			override def receive: PartialFunction[Any, Unit] = {
				case StopReceiver =>
					logInfo("接收到停止接收器的消息")
					ReceiverSupervisorImpl.this.stop("Driver要求停止接收器", None)
				case CleanupOldBlocks(threshTime) =>
					logInfo("接收到清理过期batch的消息")
					logInfo(s"开始清理在${threshTime}之前blocks......")
					receivedBlockHandler.cleanupOldBlocks(threshTime.milliseconds)
				case UpdateRateLimit(eps) =>
					logInfo(s"接收到更新速度的消息rate=:${eps}")
					registeredBlockGenerators.foreach{bg =>
						bg.updateRate(eps)
					}
			}
		}
	)

	/**  用于产生唯一的block id标识 */
	private val newBlockId = new AtomicLong(System.currentTimeMillis())

	/** 所有已注册的Block Generator */
	private val registeredBlockGenerators = new ArrayBuffer[BlockGenerator] with mutable.SynchronizedBuffer[BlockGenerator]

	/** 将接收的数据划分为数据块，以便往BlockManager中存储 */
	private val defaultBlockGeneratorListener = new BlockGeneratorListener {
		override def onAddData(data: Any, metadata: Any): Unit = {}

		override def onGenerateBlock(blockId: StreamBlockId): Unit = {}

		override def onPushBlock(blockId: StreamBlockId, arrayBuffer: ArrayBuffer[_]): Unit = {
			pushArrayBuffer(arrayBuffer, None, Some(blockId))
		}

		override def onError(message: String, cause: Throwable): Unit = {
			reportError(message, cause)
		}
	}

	private val defaultBlockGenerator = createBlockGenerator(defaultBlockGeneratorListener)

	/**  创建新的唯一的block id */
	private def nextBlockId = StreamBlockId(receiver.streamId, newBlockId.getAndIncrement())

	private def cleanupOldBlocks(cleanupThreshTime: Time): Unit = {
		logInfo(s"开始清理${cleanupThreshTime}之前的blocks")
		receivedBlockHandler.cleanupOldBlocks(cleanupThreshTime.milliseconds)
	}

	/*
	 * =================
	 * 覆写方法
	 * =================
	 */

	/** 获取默认block generator的处理速率 */
	override def getCurrentRateLimit: Long = defaultBlockGenerator.getCurrentLimit

	/** 存储将某个数据对象 */
	override def pushSingle(data: Any): Unit = {
		defaultBlockGenerator.addData(data)
	}

	/** 将接收的字节作为一个Block存储在Spark Memory中 */
	override def pushBytes(bytes: ByteBuffer, optionalMetadata: Option[Any], optionalBlockId: Option[StreamBlockId]): Unit = {
		pushAndReportBlock(ByteBufferBlock(bytes), optionalMetadata,optionalBlockId)
	}

	/** 将迭代器中的内容作为一个Block存储在Spark Memory中  */
	override def pushIterator(iter: Iterator[_], optionalMetadata: Option[Any], optionalBlockId: Option[StreamBlockId]): Unit = {
		pushAndReportBlock(IteratorBlock(iter), optionalMetadata, optionalBlockId)
	}

	/** 将数组中的内容作为一个Block存储在Spark Memory中  */
	override def pushArrayBuffer(arrBuffer: ArrayBuffer[_], optionalMetadata: Option[Any], optionalBlockId: Option[StreamBlockId]): Unit = {
		pushAndReportBlock(ArrayBufferBlock(arrBuffer), optionalMetadata,optionalBlockId)
	}

	/**
		* 存储block到BlockManager同时向Driver报告
		*
		* @param receivedBlock 三种类型：ArrayBufferBlock、IteratorBlock、ByteBufferBlock
		* @param metadata
		* @param blockIdOption
		*/
	def pushAndReportBlock(receivedBlock: ReceivedBlock, metadata: Option[Any], blockIdOption: Option[StreamBlockId]): Unit = {
		val blockId = blockIdOption.getOrElse(nextBlockId)
		val startTime = System.currentTimeMillis()
		val blockStoreResult = receivedBlockHandler.storeBlock(blockId, receivedBlock)
		logInfo(s"存储block=${blockId}所用时间：${System.currentTimeMillis() - startTime}")

		val numRecords = blockStoreResult.numRecords
		val blockInfo = ReceivedBlockInfo(blockId.streamId,numRecords,metadata, blockStoreResult)
		trackerEndpoint.askWithRetry[Boolean](AddBlock(blockInfo))
		logInfo(s"向Driver中的ReceiverTracker报告已添加block(${blockInfo})")
	}

	/** 报告错误 */
	override def reportError(message: String, cause: Throwable): Unit = {
		val errString = Option(cause).map(Throwables.getStackTraceAsString).getOrElse("")
		trackerEndpoint.send(ReportError(receiver.streamId, message, errString))
		logWarning("接收器端出现错误:{}，向Driver(ReceiverTracker)报告")
	}

	/**
		* 创建一个自定义的[[BlockGenerator]]，接收器通过[[BlockGeneratorListener]]
		* 基于异步事件进行控制。
		*
		* PS：不要显示调用`BlockGenerator`的start和stop方法，而是由`ReceiverSupervisorImpl`
		* 负责管理。
		*
		*/
	override def createBlockGenerator(blockGeneratorListener: BlockGeneratorListener): BlockGenerator = {
		//清理所有已经停止的BlockGenerators
		registeredBlockGenerators --= registeredBlockGenerators.filter(_.isStopped())

		/**  一个BlockGenerator只能服务于一个InputDStream */
		val newBlockGenerator = new BlockGenerator(blockGeneratorListener, receiver.streamId, env.conf)
		registeredBlockGenerators += newBlockGenerator
		newBlockGenerator
	}

	override protected def onStart(): Unit = {
		registeredBlockGenerators.foreach(_.start())
	}

	override protected def onStop(msg: String, error: Option[Throwable]): Unit = {
		registeredBlockGenerators.foreach(_.stop())
		env.rpcEnv.stop(endpoint)
	}

	/**
		* 当接收器被成功启动 `前` 被调用
		*
		* @return true表示Driver接收当前
		*/
	override protected def onReceiverStart(): Boolean = {
		val msg = RegisterReceiver(receiver.streamId, receiver.getClass.getSimpleName,host, executorId,endpoint)
		trackerEndpoint.askWithRetry[Boolean](msg)
	}

	override protected def onReceiverStop(msg: String, error: Option[Throwable]): Unit = {
		logInfo(s"向Driver发送注销receiver[{${receiver.streamId}]的消息")
		val errString = error.map(Throwables.getStackTraceAsString).getOrElse("")
		trackerEndpoint.askWithRetry[Boolean](DeregisterReceiver(receiver.streamId, msg, errString))
		logInfo(s"停止receiver:${receiver.streamId}")
	}
}
