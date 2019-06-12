package org.apache.spark.streaming.receiver

import java.nio.ByteBuffer
import java.util.concurrent.CountDownLatch

import org.apache.spark.storage.StreamBlockId
import org.apache.spark.util.ThreadUtils
import org.apache.spark.{Logging, SparkConf}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/**
	* 抽象类
	* 负责监控worker node上接收器，包含所有处理接收器接收到数据的处理接口
	*
	* @param receiver
	* @param conf
	*/
private[streaming] abstract class ReceiverSupervisor(receiver: Receiver[_], conf: SparkConf) extends Logging {

	object ReceiverState extends Enumeration {
		type CheckpointState = Value
		val Initialized, Started, Stopped = Value
	}

	import ReceiverState._

	//将接收器与监控器关联
	receiver.attachSupervisor(this)

	private val futureExecutionContext = ExecutionContext.fromExecutorService(
		ThreadUtils.newDaemonCachedThreadPool("receiver-supervisor-future", 128))

	/** 接收器关联的InputDStream标识 */
	private val streamId = receiver.streamId

	/** 标记接收器是否被停止 */
	private val stopWatch = new CountDownLatch(1)

	/** 接收器重启的延迟 */
	private val defaultRestartDelay = conf.getInt("spark.streaming.receiverRestartDelay", 2000)

	/** 当前接收器的最大接收速度 */
	private[streaming] def getCurrentRateLimit: Long = Long.MaxValue

	/** 接收器以外停止时所关联的错误 */
	@volatile
	protected var stoppingError: Throwable = null

	/** 接收器的状态 */
	@volatile
	private[streaming] var receiverState = Initialized

	/*
	* =================
	* 抽象方法，子类需要实现
	* =================
	*/

	/** 存储将某个数据对象 */
	def pushSingle(data: Any)

	/** 将接收的字节作为一个Block存储在Spark Memory中 */
	def pushBytes(bytes: ByteBuffer,
								optionalMetadata: Option[Any],
								optionalBlockId: Option[StreamBlockId])

	/** 将迭代器中的内容作为一个Block存储在Spark Memory中  */
	def pushIterator(iter: Iterator[_],
									 optionalMetadata: Option[Any],
									 optionalBlockId: Option[StreamBlockId])
	/** 将数组中的内容作为一个Block存储在Spark Memory中  */
	def pushArrayBuffer(arrBuffer: ArrayBuffer[_],
											optionalMetadata: Option[Any],
											optionalBlockId: Option[StreamBlockId])


	/**
		* 创建一个自定义的[[BlockGenerator]]，接收器通过[[BlockGeneratorListener]]
		* 基于异步事件进行控制。
		*
		* PS：不要显示调用`BlockGenerator`的start和stop方法，而是由`ReceiverSupervisorImpl`
		* 负责管理。
		*
		*/
	def createBlockGenerator(blockGeneratorListener: BlockGeneratorListener):BlockGenerator

	/** 报告错误 */
	def reportError(message: String, cause: Throwable)

	/*
	* =========================
	* 启动、停止接收器管理线程的方法
	* =========================
	*/

	/**
		* 监控线程启动时被调用。
		*
		* PS：该方法必须在receiver.onStart()方法之前被调用，以保证BlockGenerator
		* 在receiver之前启动，这样避免接收器先启动保存数据时BlockGenerator
		* 尚未创建成功造成数据丢失。
		*/
	protected def onStart() {}

	/**
		* 监控线程停止时被调用
		*
		* PS：该方法必须在receiver.onStop()方法后调用，确保接收器接收到
		* 的数据被持久化。
		*/
	protected def onStop(msg: String, error: Option[Throwable]) {}

	/**
		* 当接收器被成功启动 `前` 被调用
		* @return true表示Driver接收当前
		*/
	protected def onReceiverStart(): Boolean

	/** 当接收器被停止 `后` 被调用 */
	protected def onReceiverStop(msg: String, error: Option[Throwable]){}

	/** 启动supervisor */
	def start(): Unit = {
		onStart()
		startReceiver()
	}

	/** 启动接收器 */
	def startReceiver(): Unit = synchronized{
		try {
			if (onReceiverStart()) {
				logInfo("注册成功，Driver接收并分配启动接收器的资源")
				receiverState = Started
				receiver.onStart()
				logInfo("调用receiver.onStart()方法启动接收器")
			} else {
				stop("注册失败，Driver拒绝启动接收器", None)
			}
		} catch {
			case NonFatal(t) =>
				stop(s"启动接收器失败，streamId=${streamId}", None)
		}
	}

	/**
		* 标记supervisor、receiver为已停止
		*/
	def stop(msg:String, error: Option[Throwable]): Unit = {
		stoppingError = error.orNull
		stopReceiver(msg, error)
		onStop(msg, error)
		futureExecutionContext.shutdown()
		stopWatch.countDown()
	}

	def stopReceiver(msg: String, cause: Option[Throwable]): Unit = synchronized{
		try {
			logInfo(s"停止supervisor和receiver：${msg}，error=${cause.getOrElse("")}")
			receiverState match {
				case Initialized =>
					logWarning("接收器尚未启动，跳过停止操作！")
				case Started =>
					receiverState = Stopped
					receiver.onStop()
					logInfo("调用receiver.onStop方法停止接收器")
					onReceiverStop(msg, cause)
				case Stopped =>
					logInfo("接收器已经停止")
			}
		} catch {
			case NonFatal(t) =>
				logError(s"停止接收器时出错streamId=${streamId}：${t.getStackTrace}")
		}
	}

	/** 在`defaultRestartDelay`后，重启接收器 */
	def restartReceiver(msg:String, error: Option[Throwable] = None): Unit = {
		restartReceiver(msg, error, defaultRestartDelay)
	}

	/**
		* 重启接收器是阻塞式操作，因此需要另起线程用于重启接收器！
		*/
	def restartReceiver(msg:String, error: Option[Throwable], delay: Int): Unit = Future{
		logWarning(s"${delay}ms后，重启接收器，原因：${msg} ${error.getOrElse("")}")
		stopReceiver(s"${delay}ms后，重启接收器，原因：${msg} ${error.getOrElse("")}", error)
		logInfo(s"睡眠${delay}ms后，重启接收器")
		Thread.sleep(delay)
		logInfo("准备重启接收器")
		startReceiver()
		logInfo("接收器已启动")
	}(futureExecutionContext)


	def isReceiverStarted: Boolean = {
		logInfo(s"接收器状态：${receiverState}")
		receiverState == Started
	}

	def isReceiverStopped: Boolean = {
		logInfo(s"接收器状态：${receiverState}")
		receiverState == Stopped
	}

	/**
		* wait the thread until the supervisor is stopped
		*/
	def awaitTermination(): Unit = {
		logInfo("挂起supervisor直到接收器停止")
		stopWatch.await()
		if(stoppingError != null) {
			logError(s"接收器异常停止，原因:${stoppingError}")
		} else {
			logInfo("接收器正常停止")
		}

		if (stoppingError != null) {
			throw stoppingError
		}
	}
}
