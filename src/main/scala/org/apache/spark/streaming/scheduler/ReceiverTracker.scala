package org.apache.spark.streaming.scheduler

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.spark.rdd.RDD
import org.apache.spark.rpc._
import org.apache.spark.scheduler.{ExecutorCacheTaskLocation, TaskLocation}
import org.apache.spark.streaming.domain.{ReceivedBlockInfo, ReceiverErrorInfo, ReceiverTrackingInfo}
import org.apache.spark.streaming.receiver.{AllReceiverIds, _}
import org.apache.spark.streaming.wal.WriteAheadLogUtils
import org.apache.spark.streaming.{SContext, Time}
import org.apache.spark.util.{SerializableConfiguration, ThreadUtils, Utils}
import org.apache.spark.{Logging, SparkContext, TaskContext}

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
	* ReceiverTracker负责Receiver的生命周期管理，包括启动、回收、执行
	* 过程中接收数据的管理、容错（重启）。ReceiverTracker在JobScheduler
	* .start方法中建立。
	*
	* ReceiverTracker需要在SContext.start启动后被调用，因为它需要所
	* 有InputDStream的集合来进行初始化。
	*
	* @skip skipReceiverLaunch 不要启动Receiver，在测试时有用。
	*
	*/
class ReceiverTracker(ssc: SContext, skipReceiverLaunch: Boolean = false) extends Logging {

	/** ReceiverTracker的状态 */
	object TrackerState extends Enumeration {
		type TrackerState = Value
		val Initialized, Started, Stopping, Stopped = Value
	}

	import TrackerState._

	private val receiverInputDStreams = ssc.graph.getReceiverInputStreams()
	private val receiverInputDStreamIds = receiverInputDStreams.map(_.inputDStreamId)

	private val receivedBlockTracker = new ReceivedBlockTracker(
		ssc.sparkContext.conf,
		ssc.sparkContext.hadoopConfiguration,
		receiverInputDStreamIds,
		ssc.scheduler.clock,
		ssc.isCheckpointPresent,
		Option(ssc.checkpointDir)
	)

	private val listenerBus = ssc.scheduler.listenerBus

	/** protected by `TrackerStateLock` */
	@volatile private var state = Initialized

	/** 若为非空表示tracker已经启动且尚未停止 */
	private var endpoint: RpcEndpointRef = null

	/** Receiver调度策略 */
	private val schedulingPolicy = new ReceiverSchedulingPolicy()

	/**
		* 用于记录当前应用程序中活动的receiver的个数，当所有receiver job
		* 都停止后，就会调用countDown方法。
		*/
	private val receiverJobExitLatch = new CountDownLatch(receiverInputDStreamIds.size)

	/**
		* 跟踪所有receiver运行时信息，key为receiver id，该数据结构只会
		* 被ReceiverTrackerEndpoint访问。
		*/
	private val receiverTrackingInfos = new mutable.HashMap[Int, ReceiverTrackingInfo]

	/**
		* 存放所有receiver preferred locations，用于对receiver job
		* 进行调度。该信息只会被ReceiverTrackerEndpoint访问。
		*/
	private val receiverPreferredLocations = new mutable.HashMap[Int, Option[String]]


	/*=================
	 * 启动、关停方法
	 *=================
	 */

	/**
		* 因为ReceiverTracker负责监控、管理所有Receiver的生命周期，因此
		* 所有Receiver需要向ReceiverTracker汇报状态信息。
		*
		* 1、ReceiverTracker与Receiver进行RPC通信需要使用Endpoint消
		* 息通信体。
		* 2、在ReceiverTracker启动时，通过另起线程的方式实现Receiver Job
		* 的调度。
		*
		*/
	def start(): Unit = synchronized {
		if(isTrackerStarted) {
			throw new Exception("ReceiverTracker已经启动")
		}

		//Receiver的启动是基于InputDStream，若InputDStream为空则就不
		// 会启动
		if(!receiverInputDStreams.isEmpty) {
			endpoint  = ssc.env.rpcEnv.setupEndpoint(
				"ReceiverTracker",
				new ReceiverTrackerEndpoint(ssc.env.rpcEnv)
			)

			//一般为false，测试时使用
			if(!skipReceiverLaunch) launchReceivers()
			logInfo("ReceiverTracker已启动")
			state = Started
		}
	}

	/** 停止Receiver的执行线程 */
	def stop(graceful: Boolean): Unit = synchronized{
		if(isTrackerStarted) {
			//1、停止receivers
			state = Stopping
			if(!skipReceiverLaunch) {
				//给所有receiver发送停止信息
				endpoint.askWithRetry[Boolean](StopAllReceivers)

				//等待运行receivers的spark作业执行完毕
				receiverJobExitLatch.await(10, TimeUnit.SECONDS)

				if(graceful) {
					logInfo("等待所有receiver jobs执行完毕....")
					receiverJobExitLatch.await()
					logInfo("所有receiver jobs以运行结束！")
				}

				//检查是否所有的receiver都已经注销
				val receivers = endpoint.askWithRetry[Seq[Int]](AllReceiverIds)
				if(receivers.nonEmpty) {
					logWarning(s"并不是所有的receivers都已经注销：${receivers}")
				} else {
					logInfo("所有的receiver都已经注销")
				}
			}

			//2、停止消息循环体endpoint
			ssc.env.rpcEnv.stop(endpoint)
			endpoint = null

			receivedBlockTracker.stop()
			logInfo("ReceiverTracker已经停止！")
			state = Stopped
		}
	}

	/** 更新receiver最大接收速率 */
	def sendRateUpdate(streamId: Int, newRate: Long): Unit = {
		if(isTrackerStarted) {
			endpoint.send(UpdateReceiverRateLimit(streamId, newRate))
		}
	}

	private def isTrackerStarted: Boolean = state == Started

	private def isTrackerStopping: Boolean = state == Stopping

	private def isTrackerStopped: Boolean = state == Stopped

	/** 获取所有活跃的executor位置信息，排除Driver所在host */
	private def getExecutors: Seq[ExecutorCacheTaskLocation] = {
		if (ssc.sc.isLocal) {
			val blockManagerId = ssc.sparkContext.env.blockManager.blockManagerId
			Seq(ExecutorCacheTaskLocation(blockManagerId.host, blockManagerId.executorId))
		} else {
			ssc.sparkContext.env.blockManager.master.getMemoryStatus.filter {
				case (blockManagerId, _) =>
					//过滤掉Driver所在机器
					blockManagerId.executorId != SparkContext.DRIVER_IDENTIFIER
			}.map { case (blockManagerId, _) =>
				ExecutorCacheTaskLocation(blockManagerId.host, blockManagerId.executorId)
			}.toSeq
		}
	}

	private def updateReceiverScheduledExecutors(
				receiverId: Int, scheduledLocations: Seq[TaskLocation]): Unit = {

		val newReceiverTrackingInfo = receiverTrackingInfos.get(receiverId) match {
			case Some(oldInfo) =>
				oldInfo.copy(state = ReceiverState.SCHEDULED,
					scheduledLocations = Some(scheduledLocations))
			case None =>
				ReceiverTrackingInfo(
					receiverId,
					ReceiverState.SCHEDULED,
					Some(scheduledLocations),
					runningExecutor = None
				)
		}
		receiverTrackingInfos.put(receiverId, newReceiverTrackingInfo)
	}

	private def reportError(streamId: Int, message: String, error: String): Unit = {
		val newReceiverTrackingInfo = receiverTrackingInfos.get(streamId) match {
			case Some(oldInfo) =>
				val errorInfo = ReceiverErrorInfo(message, error, oldInfo.errorInfo.map(_.lastErrorTime).getOrElse(-1L))
				oldInfo.copy(errorInfo=Some(errorInfo))
			case None =>
				logWarning("no prior receiver info")
				val err  = ReceiverErrorInfo(message, error, ssc.scheduler.clock.getTimeMillis())
				ReceiverTrackingInfo(streamId, ReceiverState.INACTIVE, None,None,None,None,Some(err))
		}

		receiverTrackingInfos(streamId) = newReceiverTrackingInfo
		listenerBus.post(SListenerReceiverError(newReceiverTrackingInfo.toReceiverInfo))

		val messageWithError = if(error != null && !error.isEmpty) {
			s"$message - $error"
		} else {
			s"$message"
		}
		logWarning(s"receiver[${streamId}]报告错误：${messageWithError}")
	}

	private def registerReceiver(
			streamId: Int,
			typ: String,
			host: String,
			executorId: String,
			receiverEndpoint: RpcEndpointRef,
			senderAddress: RpcAddress):Boolean = {

		if(!receiverInputDStreamIds.contains(streamId)) {
			throw new Exception(s"尝试注册不存在的streamId: ${streamId}")
		}

		if(isTrackerStopping || isTrackerStopped) {
			return false
		}

		val scheduledLocations = receiverTrackingInfos(streamId).scheduledLocations
		val acceptableExecutors = if(scheduledLocations.nonEmpty) {
			//使用ReceiverSchedulingPolicy.scheduleReceiver推荐的位置
			scheduledLocations.get
		} else {
			//调用ReceiverSchedulingPolicy.rescheduleReceiver重新获
			// 取可以运行Receiver的位置信息
			scheduleReceiver(streamId)
		}

		def isAcceptable:Boolean = acceptableExecutors.exists{
			case loc: ExecutorCacheTaskLocation=>loc.executorId == executorId
			case loc: TaskLocation =>loc.host == host
		}

		if(!isAcceptable) {
			//refuse it since it's scheduled to a wrong executor
			false
		} else {
			val name =s"${typ}-${streamId}"

			val receiverTrackingInfo = ReceiverTrackingInfo(
				streamId,
				ReceiverState.ACTIVE,
				scheduledLocations  = None,
				runningExecutor = Some(ExecutorCacheTaskLocation(host, executorId)),
				name = Some(name),
				endpoint = Some(receiverEndpoint)
			)

			receiverTrackingInfos.put(streamId, receiverTrackingInfo)
			listenerBus.post(SListenerReceiverStarted(receiverTrackingInfo.toReceiverInfo))
			logInfo(s"已注册来自${senderAddress}的Receiver[${streamId}]")
			true
		}
	}

	private def scheduleReceiver(streamId: Int):Seq[TaskLocation] = {
		val prederredLocation = receiverPreferredLocations.getOrElse(streamId, None)
		val scheduledLocations = schedulingPolicy.rescheduleReceiver(streamId, prederredLocation, receiverTrackingInfos, getExecutors)
		updateReceiverScheduledExecutors(streamId, scheduledLocations)
		scheduledLocations
	}

	/** 注销一个receiver */
	private def deregisterReceiver(streamId: Int, message: String, error: String): Unit = {
		val lastErrorTime = if(error == null || error == "") -1L else ssc.scheduler.clock.getTimeMillis()

		val errorInfo = ReceiverErrorInfo(message, error, lastErrorTime)
		val newReceiverTrackingInfo = receiverTrackingInfos.get(streamId) match {
			case Some(oldInfo) =>
				oldInfo.copy(
					state = ReceiverState.INACTIVE,
					errorInfo = Some(errorInfo)
				)
			case None =>
				logWarning("No Prior Receiver Info")
				ReceiverTrackingInfo(streamId,ReceiverState.INACTIVE, None,None,None,None,Some(errorInfo))
		}

		receiverTrackingInfos(streamId) = newReceiverTrackingInfo
		listenerBus.post(SListenerReceiverStopped(newReceiverTrackingInfo.toReceiverInfo))

		val messageWithError = if(error != null && !error.isEmpty) {
			s"${message}-${error}"
		} else {
			s"${message}"
		}
		logInfo(s"注销receiver[${streamId}]：${messageWithError}")
	}


	/*======================================
	 * Facade Pattern：ReceivedBlockTracker
	 *======================================
	 */
	/** 将blocks添加到stream中 */
	private def addBlock(info: ReceivedBlockInfo):Boolean = {
		receivedBlockTracker.addBlock(info)
	}

	/** 将所有未分配的blocks都分配给当前批次 */
	def allocateBlocksToBatch(batchTime:Time):Unit =  {
		if(receiverInputDStreams.nonEmpty) {
			receivedBlockTracker.allocateBlockToBatch(batchTime)
		}
	}

	/** 所有某个批次下所有streams对应的blocks */
	def getBlocksOfBatch(batchTime:Time): Map[Int, Seq[ReceivedBlockInfo]] = {
		receivedBlockTracker.getBlocksOfBatch(batchTime)
	}

	/** 获取某个批次下某个stream对应的所有blocks */
	def getBlocksOfBatchAndStream(batchTime:Time, streamId:Int):Seq[ReceivedBlockInfo] = {
		receivedBlockTracker.getBlocksOfBatchAndStreamId(batchTime, streamId)
	}

	/** 请求当前threshold时间之前的所有批次的blocks的数据和元数据 */
	def cleanupOldBlockAndBatches(cleanupThreshTime:Time): Unit = {
		//清理block data and metadata
		receivedBlockTracker.cleanupOldBatches(cleanupThreshTime, false)

		//给Receiver发送信息，清理就的blocks data
		if(WriteAheadLogUtils.enableReceiverLog(ssc.conf))  {
			logInfo(s"清理Receiver中在${cleanupThreshTime}之前的batch data")
			synchronized  {
				if(isTrackerStarted) {
					endpoint.send(CleanupOldBlocks(cleanupThreshTime))
				}
			}
		}
	}

	/** 是否仍然有未被处理blocks*/
	def hasUnallocatedBlocks:Boolean = {
		receivedBlockTracker.hasUnallocatedReceivedBlocks
	}


	/**
		* 在进行receiver调度之前，先运行一个dummu作业，确保Spark中的所有
		* Worker Node都已经注册。避免所有的receivers都被调度到同一个节
		* 点上。
		*
		* fixme
		* 一般应该获取executors的个数，然后根据如下配置：
		* spark.scheduler.minRegisteredResourcesRatio
		* spark.scheduler.maxRegisteredResourcesWaitingTime
		* 等待所有executors都已注册后才启动。
		*
		*/
	private def runDummySparkJob(): Unit = {
		if(!ssc.sparkContext.isLocal) {
			ssc.sparkContext.makeRDD(1 to 50, 50)
				.map(x => (x, 1))
  			.reduceByKey(_+_, 20)
  			.collect()
		}
		assert(getExecutors.nonEmpty)
	}

	/**
		* 1、从InputDStream中获取所有的Receivers；
		* 2、将Receivers均匀分配给Worker Node上并行执行；
		*
		* ReceiverInputDStreams是在Driver端，可以认为是Receiver的元
		* 数据。
		*
		*/
	private def launchReceivers(): Unit = {
		//一个InputDStream只产生一个Receiver
		val receivers = receiverInputDStreams.map{ ris =>
			val rcvr  = ris.getReceiver()
			rcvr.setReceiverId(ris.inputDStreamId)
			rcvr
		}

		//资源预热，确保资源均匀被分配
		runDummySparkJob()

		logInfo(s"准备启动${receivers.size}个Receviers")
		endpoint.send(StartAllReceivers(receivers))
	}

	/*==========================
	 * Receiver Tracker Endpoint
	 *==========================
	 */
	/**
		* 用于实现与Receiver的通信
		*/
	private class ReceiverTrackerEndpoint(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint {

		private val submitJobThreadPool = ExecutionContext.fromExecutorService(
			ThreadUtils.newDaemonCachedThreadPool("submit-job-thread-pool")
		)

		private val walBatchingThreadPool = ExecutionContext.fromExecutorService(
			ThreadUtils.newDaemonCachedThreadPool("wal-batching-thread-pool")
		)

		@volatile private var active: Boolean = true

		override def receive: PartialFunction[Any, Unit] = {
			case StartAllReceivers(receivers) =>
				val scheduledLocations = schedulingPolicy.scheduleReceivers(receivers, getExecutors)
				for (receiver <- receivers) {
					val executors = scheduledLocations(receiver.streamId)
					updateReceiverScheduledExecutors(receiver.streamId, executors)
					receiverPreferredLocations(receiver.streamId) = receiver.preferredLocation
					//启动receiver
					startReceiver(receiver, executors)
				}
			case RestartReceiver(receiver: Receiver[_]) =>
				//在重试启动Receiver时会遍历所有Receiver可能运行的Executors
				// 最大程度的保证负责均衡并且增加Receiver运行成功的机会。

				//首先需要去掉inactive的executors
				val oldScheduledExecutors = getStoredScheduledExecutors(receiver.streamId)
				val scheduledLocations = if (oldScheduledExecutors.nonEmpty) {
					//try global scheduling again
					oldScheduledExecutors
				} else {
					//若没有可选的executor运行receiver，则重新调度
					val oldReceiverInfo = receiverTrackingInfos(receiver.streamId)
					//清理掉 `scheduledLocations`中内容，告诉调度器进行 local scheduling
					val newReceiverInfo = oldReceiverInfo.copy(
						state = ReceiverState.INACTIVE, scheduledLocations = None)

					receiverTrackingInfos(receiver.streamId) = newReceiverInfo

					//do local scheduling
					schedulingPolicy.rescheduleReceiver(
						receiver.streamId, receiver.preferredLocation,
						receiverTrackingInfos, getExecutors
					)
				}

				// 假设每次只能重启一个receiver，因此这里不需要更新receiverTrackingInfos
				startReceiver(receiver, scheduledLocations)
			case c: CleanupOldBlocks =>
				receiverTrackingInfos.values.flatMap(_.endpoint).foreach(_.send(c))
			case UpdateReceiverRateLimit(streamId, newRate) =>
				for (info <- receiverTrackingInfos.get(streamId); endpoint <- info.endpoint) {
					endpoint.send(UpdateRateLimit(newRate))
				}
			case ReportError(streamId, msg, err) =>
				//报告receiver发送过来的错误信息
				reportError(streamId, msg, err)
		}

		override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
			case RegisterReceiver(streamId, tpy, host, executorId, receiverEndpoint) =>
				val successful = registerReceiver(streamId, tpy, host, executorId,receiverEndpoint, context.senderAddress)
				context.reply(successful)


			case AddBlock(receivedBlockInfo) =>
				if(WriteAheadLogUtils.isBatchingEnabled(ssc.conf,isDriver = true)) {
					walBatchingThreadPool.execute(new Runnable {
						override def run(): Unit = Utils.tryLogNonFatalError{
							if(active) {
								context.reply(addBlock(receivedBlockInfo))
							} else {
								throw new IllegalStateException("ReceiverTracker RpcEndpoint已关闭！")
							}
						}
					})
				} else {
					context.reply(addBlock(receivedBlockInfo))
				}

			case DeregisterReceiver(streamId, msg, error)=>
				deregisterReceiver(streamId, msg, error)
				context.reply(true)

			//local message
			case AllReceiverIds =>
				context.reply(receiverTrackingInfos.filter(_._2.state == ReceiverState.ACTIVE).keys.toSeq)

			case StopAllReceivers =>
				assert(isTrackerStopping || isTrackerStopped)
				stopReceivers()
				context.reply(true)

		}

		/**
			* 根据调度的位置启动receiver
			*
			* TaskLocation表示位置是机器级别，而不是Executor级别！
			*/
		private def startReceiver(receiver: Receiver[_], scheduledLocations: Seq[TaskLocation]):Unit = {
			def shouldStartReceiver:Boolean = {
				//当状态为Initialized或Started状态时，返回true
				!(isTrackerStopped || isTrackerStopping)
			}

			val receiverId = receiver.streamId
			if(!shouldStartReceiver) {
				onReceiverJobFinish(receiverId)
				return
			}

			val checkpointDirOption = Option(ssc.checkpointDir)
			val serializableHadoopConf = new SerializableConfiguration(ssc.sparkContext.hadoopConfiguration)

			//在worker上启动receiver的
			val startReceiverFunc:Iterator[Receiver[_]]=>Unit =
				(iterator:Iterator[Receiver[_]])=> {
					if(!iterator.hasNext) {
						throw new Exception("无法启动Receiver，因为没有可启动的Receiver对象")
					}

					if(TaskContext.get().attemptNumber() == 0) {
						val receiver = iterator.next()
						assert(iterator.hasNext == false)

						val supervisor = new ReceiverSupervisorImpl(receiver, ssc.sc.env, serializableHadoopConf.value, checkpointDirOption)
						//启动receiver
						supervisor.start()
						supervisor.awaitTermination()
					} else {
						//属于重启receiver操作，重启Receiver是有Spark Task重
						// 试机制实现，因此直接退出即可
					}
				}

			//以receiver和scheduledLocations创建RDD，通过RDD Action操
			// 作触发Spark Job从而实现receiver启动
			val receiverRDD: RDD[Receiver[_]] = if(scheduledLocations.isEmpty) {
				ssc.sc.makeRDD(Seq(receiver), 1)
			} else {
				val preferredLocations = scheduledLocations.map(_.toString).distinct
				ssc.sc.makeRDD(Seq(receiver -> preferredLocations))
			}

			//每个RDD中只有一条数据：receiver
			receiverRDD.setName(s"Receiver ${receiverId}")
			ssc.sparkContext.setJobDescription(s"Steaming Job running receiver ${receiverId}")
			ssc.sparkContext.setCallSite(Option(ssc.getStartSite()).getOrElse(Utils.getCallSite()))

			/*
				每一个Receiver启动都是运行一个Spark Core的Job作业来启动。

				为什么要启动一个Spark Core作业来启动Receiver？
				1、确保Receiver一定会被启动，若启动失败则发送RestartReceiver
				可以无限重启，而Spark Core的Task、Stage重试次数有限。

				2、负载均衡，每个Spark Job启动一个Receiver实例比较平均，不
				会出现在同一台机器上出现两个Receiver的情况，而如果是Task启动
				则可能出现这种情况。这样可以保证Receiver的吞吐量！

				问题：假设Spark Streaming应用程序有10个InputDStream，就会
				产生10个Receiver，这里的submitJob中的作业是启动一个Receiver
				还是一次性把所有的Receiver一起启动？
				由于receiverRDD中只有一个元素，因此每一次提交一个Spark Job
				只是启动一个Receiver。

				采用一个Job把所有Receiver都启动的问题？
				1、多个Receiver可能运行在同一个Executor上，分布不均匀，可能
				会导致数据倾斜！
				2、Job失败就会导致所有Receiver失败！

				而每个Receiver采用一个Job方式启动避免了上述的两个问题且保证
				一定会启动Receiver，并且在重试启动Receiver时会遍历所有Receiver
				可能运行的Executors最大程度的保证负责均衡并且增加Receiver运
				行成功的机会。
			 */
			val future = ssc.sc.submitJob[Receiver[_], Unit,Unit](
				receiverRDD,
				startReceiverFunc,
				Seq(0),
				(_, _)  => Unit,
				()
			)

			//若作业启动失败，则不断重启直到Receiver被关闭为止
			future.onComplete{
				case Success(value) =>
					if(!shouldStartReceiver) {
						onReceiverJobFinish(receiverId)
					} else {
						logInfo(s"Receiver已经执行结束，再次重启Receiver[${receiverId}]")
						self.send(RestartReceiver(receiver))
					}
				case Failure(exception) =>
					if(!shouldStartReceiver) {
						onReceiverJobFinish(receiverId)
					} else {
						logError(s"Receiver以外终止：${exception}，尝试重启...")
						self.send(RestartReceiver(receiver))
					}
			}(submitJobThreadPool)

			logInfo(s"成功启动Receiver[${receiverId}]")
		}

		/** 给receiver发送stop signal */
		private def stopReceivers(): Unit ={
			receiverTrackingInfos.values.flatMap(_.endpoint).foreach(_.send(StopReceiver))
			logInfo(s"发送StopReceiver消息给${receiverTrackingInfos.size}个Receiver")
		}

		override def onStop(): Unit = {
			submitJobThreadPool.shutdown()
			active = false
			walBatchingThreadPool.shutdown()
		}

		/** 当receiver停止时被调用，不会重启receiver对应的Spark Job*/
		private def onReceiverJobFinish(receiverId:Int): Unit = {
			receiverJobExitLatch.countDown()
			receiverTrackingInfos.remove(receiverId).foreach{info=>
				if (info.state == ReceiverState.ACTIVE)  {
					logWarning(s"receiver[${receiverId}]已停止当尚未注销！")
				}
			}
		}

		/** 获取本地存储的且还是active的executor的位置信息 */
		private def getStoredScheduledExecutors(receiverId: Int): Seq[TaskLocation] = {
			if (receiverTrackingInfos.contains(receiverId)) {
				val scheduledLocations = receiverTrackingInfos(receiverId).scheduledLocations
				if (scheduledLocations.nonEmpty) {
					val executors = getExecutors.toSet
					scheduledLocations.get.filter {
						case loc: ExecutorCacheTaskLocation => executors.contains(loc)
						case loc: TaskLocation => true
					}
				} else {
					Nil
				}
			} else {
				Nil
			}
		}
	}
}
