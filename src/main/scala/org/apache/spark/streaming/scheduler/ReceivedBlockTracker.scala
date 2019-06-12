package org.apache.spark.streaming.scheduler

import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.domain.ReceivedBlockInfo
import org.apache.spark.streaming.wal.{WriteAheadLog, WriteAheadLogUtils}
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.util.{GClock, Utils}

import scala.collection.mutable
import scala.util.control.NonFatal

/*==========================================================
 *  更新ReceivedBlockTracker的事件
 *==========================================================
 */
sealed trait ReceivedBlockTrackerLogEvent

case class BlockAdditionEvent(receivedBlockInfo: ReceivedBlockInfo)
	extends ReceivedBlockTrackerLogEvent

case class BatchAllocationEvent(time:Time, allocatedBlocks: AllocatedBlocks)
  extends ReceivedBlockTrackerLogEvent

case class BatchCleanupEvent(times: Seq[Time])
	extends ReceivedBlockTrackerLogEvent

/*==========================================================
 * 代表分配给一个Batch批次的所有Blocks信息
 *==========================================================
 */
case class AllocatedBlocks(streamIdsToAllocatedBlocks: Map[Int, Seq[ReceivedBlockInfo]]) {
	def getBlocksOfStream(streamId: Int):Seq[ReceivedBlockInfo] = {
		streamIdsToAllocatedBlocks.getOrElse(streamId, Seq.empty)
	}
}

/**
	* 用于管理所有接收的Blocks，并在需要时将它们分给某一个批次Batch。该
	* 实例的所有操作都可以通过WAL保存到日志中(如果提供了checkpoint dir)，
	* 以便在Driver失效后重启能够恢复：
	* 	1、之前已接收的Blocks
	* 	2、block-to-batch分配关系
	*
	* 当执行checkpoint dir目录后，该实例会尝试从目录中读取事件。
	*
	*/
class ReceivedBlockTracker(
		conf: SparkConf,
		hadoopConf: Configuration,
		streamIds: Array[Int],
		clock: GClock,
		recoverFromWriteAheadLog: Boolean,
		checkpointDirOption: Option[String]) extends Logging{

	private type ReceivedBlockQueue = mutable.Queue[ReceivedBlockInfo]

	private val streamIdToUnallocatedBlockQueues = new mutable.HashMap[Int, ReceivedBlockQueue]
	private val timeToAllocatedBlocks = new mutable.HashMap[Time, AllocatedBlocks]()
	private val writeAheadLogOption = createWriteAheadLog()

	private var lastAllocatedBatchTime:Time = null

	if(recoverFromWriteAheadLog) {
		recoverPastEvents()
	}

	/** 添加接收到Block，若开启WAL先写WAL日志文件 */
	def addBlock(receivedBlockInfo: ReceivedBlockInfo):Boolean = {
		try {
			val writeResult = writeToLog(BlockAdditionEvent(receivedBlockInfo))
			if (writeResult) {
				synchronized {
					getReceivedBlockQueue(receivedBlockInfo.streamId) += receivedBlockInfo
				}
				logInfo(s"InputDStream${receivedBlockInfo.streamId}已接收Block${receivedBlockInfo.blockId}")
			} else {
				logInfo(s"InputDStream${receivedBlockInfo.streamId}已接收Block${receivedBlockInfo.blockId}预写到WAL失败！")
			}
			writeResult
		} catch {
			case NonFatal(e) =>
				logError(s"添加Block失败：${e.getMessage}")
				false
		}
	}

	/**
		* 将所有为分配的Blocks分配给{@param batchTime}所指定的Batch，
		* 若开启的WAL机制，在需要先写WAL日志。
		*
		* 根据时间间隔将该时间间隔中的数据划分为一个Batch，并将其交给该
		* Batch Interval中的作业去处理。JobGenerator在生成作业时会调
		* 用该函数获取指定Batch中的数据。
		*
		* Facade Design Pattern:
		* 	ReceiverTracker和ReceivedBlockTracker，内部实际是用后者
		* 实现Block的管理。
		*
		*/
	def allocateBlockToBatch(batchTime: Time): Unit = synchronized{
		if(lastAllocatedBatchTime == null || batchTime > lastAllocatedBatchTime) {

			val streamIdToBlocks = streamIds.map{streamId =>
				(streamId, getReceivedBlockQueue(streamId).dequeueAll(x => true))
			}.toMap

			val allocatedBlocks = AllocatedBlocks(streamIdToBlocks)
			if(writeToLog(BatchAllocationEvent(batchTime, allocatedBlocks))) {
				timeToAllocatedBlocks.put(batchTime, allocatedBlocks)
				lastAllocatedBatchTime = batchTime
			} else {
				logInfo(s"Possibly processed batch $batchTime need to be processed again in WAL recovery")
			}


		} else {
			//TODO check 这种情况仅仅会发生在恢复过程中，有两种场景：
			//1、WAL是以BatchAllocationEvent事件结束，而没有BatchCleanupEvent
			// 需要再次执行batch job or half-processed batch job以便
			// 让lastAllocatedBatchTime更新为 batchTime
			//2、Slow checkpointing使得恢复的BatchTime要早于WAL恢复
			// lastAllocatedBatchTime
			logInfo(s"可能需要再次运行WAL恢复过程，batchTime=${batchTime}")
		}

	}


	def getBlocksOfBatch(batchTime:Time):Map[Int, Seq[ReceivedBlockInfo]] = synchronized {
		timeToAllocatedBlocks.get(batchTime).map(_.streamIdsToAllocatedBlocks).getOrElse(Map.empty)
	}

	def getBlocksOfBatchAndStreamId(batchTime:Time, streamId:Int):Seq[ReceivedBlockInfo] = synchronized{
		timeToAllocatedBlocks.get(batchTime).map(_.getBlocksOfStream(streamId)).getOrElse(Seq.empty)
	}

	def hasUnallocatedReceivedBlocks:Boolean = synchronized{
		!streamIdToUnallocatedBlockQueues.values.forall(_.isEmpty)
	}

	/** 获取某个streamid接收的但尚未分配给Batch的Block */
	def getUnallocatedBlocks(streamId:Int):Seq[ReceivedBlockInfo] = synchronized{
		getReceivedBlockQueue(streamId)
	}

	/**
		* @param waitForCompletion true代表WAL将过期文件删除后才返回
		*/
	def cleanupOldBatches(cleanupThresholdTime:Time, waitForCompletion:Boolean):Unit = synchronized{
		require(cleanupThresholdTime.milliseconds < clock.getTimeMillis())
		val timeToCleanup = timeToAllocatedBlocks.keys.filter(_ < cleanupThresholdTime).toSeq
		logInfo(s"开始清理已过期的BatchBlocks：${timeToCleanup}")
		if(writeToLog(BatchCleanupEvent(timeToCleanup))) {
			timeToAllocatedBlocks --= timeToCleanup
			writeAheadLogOption.foreach(_.clean(cleanupThresholdTime.milliseconds, waitForCompletion))
		} else {
			logWarning("WAL日志清理时失败！")
		}
	}

	/** 停止Block Tracker */
	def stop(): Unit = {
		writeAheadLogOption.foreach(_.close())
	}

	/*===============
	 * private method
	 *===============
	 */

	/**
		* Driver之前失败重启后，从WAL日志中恢复所有已分配、未分配的
		* ReceivedBlockInfo信息。
		*/
	private def recoverPastEvents(): Unit ={

		def insertAddedBlock(receivedBlockInfo: ReceivedBlockInfo): Unit ={
			logInfo(s"尝试恢复：插入已添加的Block -> ${receivedBlockInfo}")
			receivedBlockInfo.setBlockIdInvalid()
			getReceivedBlockQueue(receivedBlockInfo.streamId) += receivedBlockInfo
		}

		/** 1、恢复block-to-batch关联关系；2、请求对应的received block queue */
		def insertAllocatedBatch(batchTime:Time, allocatedBlocks: AllocatedBlocks): Unit = {
			logInfo(s"尝试恢复：将${batchTime}时刻已分配的Batch添加到${allocatedBlocks.streamIdsToAllocatedBlocks}中")
			streamIdToUnallocatedBlockQueues.values.foreach(_.clear())
			timeToAllocatedBlocks.put(batchTime, allocatedBlocks)
			lastAllocatedBatchTime = batchTime
		}
		/** 清理batch allocation */
		def cleanupBatch(batchTimes: Seq[Time]): Unit = {
			logInfo(s"尝试恢复：清理在 ${batchTimes}时刻AllocatedBlocks")
			timeToAllocatedBlocks --= batchTimes
		}

		writeAheadLogOption.foreach{wal=>
			logInfo(s"尝试从WAL日志文件${checkpointDirOption.get}中恢复Block信息")
			wal.readAll().foreach{byteBuffer =>
				logInfo(s"尝试恢复记录：${byteBuffer}")
				Utils.deserialize[ReceivedBlockTrackerLogEvent](byteBuffer.array(), Thread.currentThread().getContextClassLoader) match {
					case BlockAdditionEvent(receivedBlockInfo) =>
						insertAddedBlock(receivedBlockInfo)
					case BatchAllocationEvent(time, allocatedBlocks) =>
						insertAllocatedBatch(time, allocatedBlocks)
					case BatchCleanupEvent(times) =>
						cleanupBatch(times)
				}
			}
		}

	}

	private def getReceivedBlockQueue(streamId:Int):ReceivedBlockQueue = {
		streamIdToUnallocatedBlockQueues.getOrElseUpdate(streamId, new ReceivedBlockQueue)
	}

	/** 若提供了checkpoint dir则创建WAL实例 */
	private def createWriteAheadLog():Option[WriteAheadLog] = {
		checkpointDirOption.map{checkpointDir =>
			val logDir = ReceivedBlockTracker.checkpointDirToLogDir(checkpointDir)
			WriteAheadLogUtils.createLogForDriver(conf, logDir, hadoopConf)
		}
	}

	private def writeToLog(record:ReceivedBlockTrackerLogEvent):Boolean =  {
		if(isWriteAheadLogEnabled) {
			logInfo(s"WAL写入记录：${record}")
			try {
				writeAheadLogOption.get.write(ByteBuffer.wrap(Utils.serialize(record)), clock.getTimeMillis())
				true
			} catch {
				case NonFatal(e) =>
					logWarning(s"写入记录${record}到WAL日志文件时出错：${e.getMessage}")
					false
			}
		} else {
			//不需要WAL
			true
		}
	}

	private def isWriteAheadLogEnabled:Boolean = writeAheadLogOption.nonEmpty
}

object ReceivedBlockTracker {
	def checkpointDirToLogDir(checkpointDir: String):String  = {
		new Path(checkpointDir, "receivedBlockMetaData").toString
	}
}