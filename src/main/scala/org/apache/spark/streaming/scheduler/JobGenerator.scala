package org.apache.spark.streaming.scheduler

import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.streaming.{Checkpoint, CheckpointWriter, Time}
import org.apache.spark.util._

import scala.util.{Failure, Success, Try}


/** JobGenerator中的事件，内部采用EventLoop处理*/
sealed trait JobGeneratorEvent
case class GenerateJobs(time: Time) extends JobGeneratorEvent
case class ClearMetadata(time:Time) extends JobGeneratorEvent
case class DoCheckpoint(time:Time, clearCheckpointDataLater: Boolean) extends JobGeneratorEvent
case class ClearCheckpointData(time:Time) extends JobGeneratorEvent

/**
	* 1、负责按照时间配置生成Spark Streaming Job，注意这里的Job不是Spark
	* 物理作业，而是物理作业的封装。
	* 2、负责DStream Metadata的checkpoint和清理。
	*/
private[streaming] class JobGenerator(jobScheduler: JobScheduler) extends Logging{
	logInfo("创建JobGenerator")
	private val ssc = jobScheduler.ssc
	private val conf = ssc.conf
	private val graph = ssc.graph

	val clock = {
		val clockClass = ssc.sc.conf.get("spark.streaming.clock","org.apache.spark.util.GSystemClock")
		try {
			Utils.classForName(clockClass).newInstance().asInstanceOf[GClock]
		} catch {
			case e: ClassNotFoundException =>
				throw e
		}
	}

	private val timer = new RecurringTimer(clock, ssc.graph.batchDuration.milliseconds,
		longTime => eventLoop.post(GenerateJobs(new Time(longTime))), "JobGenerator")

	/** 在JobGenerator启动时初始化，若不为空则说明JobScheduler已启动 */
	private var eventLoop:EventLoop[JobGeneratorEvent] = null

	/** 上一次Batch完成、同时执行完checkpoint和metadata cleanup的时间 */
	private var lastProcessBatch: Time = null


	/** lazy的原因是要确保在checkpoint duration被设置且JobGenerator启动后再获取 */
	private lazy val shouldChechpoint =  ssc.checkpointDir != null && ssc.checkpointDuration != null

	private lazy val checkpointWriter  = if(shouldChechpoint) {
		new CheckpointWriter(this, ssc.checkpointDir, conf, ssc.sparkContext.hadoopConfiguration)
	} else {
		null
	}

	/**
		* JobGenerator如何初始化？
		* SContext.start()会调用JobScheduler.start()方法，后者在初始
		* 化时，会调用JobGenerator.start()和ReceiverTracker.start()
		* 方法，从而实现JobGenerator和ReceiverTracker的初始化。
		*/
	def start(): Unit = synchronized{
		//已经启动
		if (eventLoop != null) return

		//在eventLoop使用之前先进行初始化，避免死锁
		checkpointWriter

		eventLoop = new EventLoop[JobGeneratorEvent]("JobGenerator") {
			override protected def onReceive(event: JobGeneratorEvent): Unit = processEvent(event)

			override protected def onError(e: Throwable): Unit = {
				jobScheduler.reportError(s"JobGenerator出错", e)
			}
		}
		eventLoop.start()

		//若不是第一次启动，则需要进行恢复
		if(ssc.isCheckpointPresent) {
			restart()
		} else {
			startFirstTime()
		}
	}


	/**
		* 停止生产作业，若processReceivedData=true，则会阻塞直到当前正
		* 在运行运业被生成、处理并checkpoint后才返回。
		*/
	def stop(processReceivedData: Boolean): Unit = synchronized{
		if(eventLoop == null) return

		if(processReceivedData) {
			logInfo("需要优雅关闭JobGenerator...")
			val timeWhenStopStarted = System.currentTimeMillis()
			val stopTimeoutMS = conf.getTimeAsMs("spark.streaming.gracefulStopTimeour",s"${10 * ssc.graph.batchDuration.milliseconds}ms")
			val pollTime = 100

			//为了避免graceful stop永久阻塞
			def hasTimedOut:Boolean = {
				val timeout = (System.currentTimeMillis() - timeWhenStopStarted) > stopTimeoutMS
				if (timeout) {
					logWarning(s"停止JobGenerator超时，timeout=${stopTimeoutMS} ms")
				}
				timeout
			}

			//阻塞直到所有received blocks被消费且对应的Job被生成
			logInfo("阻塞直到所有received blocks被消费且对应的Job被生成")
			while(!hasTimedOut && jobScheduler.receiverTracker.hasUnallocatedBlocks) {
				Thread.sleep(pollTime)
			}
			logInfo("阻塞直到所有received blocks被消费且对应的Job被生成")

			//停止产生作业
			val stopTime = timer.stop(false)
			graph.stop()
			logInfo("停止作业生成定时器")

			//等待生成的作业被处理完成
			def haveAllBatchesBeenProcessed:Boolean = {
				lastProcessBatch != null && lastProcessBatch.milliseconds == stopTime
			}

			logInfo("等待生成的作业被处理完成")
			while (!hasTimedOut && !haveAllBatchesBeenProcessed) {
				Thread.sleep(pollTime)
			}
			logInfo("等待生成的作业被处理完成")

		} else {
			logInfo("立即关闭Job Generator而不关心是否正在生成的作业")
			timer.stop(true)
			graph.stop()
		}

		if(shouldChechpoint) checkpointWriter.stop()
		eventLoop.stop()
		logInfo("成功停止JobGenerator")
	}

	/** 回调函数，当batch处理完成后被调用 */
	def onBatchCompletion(time:Time): Unit = {
		eventLoop.post(ClearMetadata(time))
	}

	/** 回调函数，当Batch的相关数据已经被checkpoint时被调用 */
	def onCheckpointCompletion(checkpointTime: Time, clearCheckpointDataLater: Boolean): Unit = {
		if(clearCheckpointDataLater) {
			eventLoop.post(ClearCheckpointData(checkpointTime))
		}
	}

	private def processEvent(event: JobGeneratorEvent): Unit = {
		logInfo(s"JobGenerator接收到事件：${event}")
		event match {
			case GenerateJobs(time) => generateJob(time)
			case ClearMetadata(time) => clearMetadata(time)
			case DoCheckpoint(time, clearCheckpointDataLater) => doCheckpoint(time, clearCheckpointDataLater)
			case ClearCheckpointData(time) => clearCheckpointData(time)
		}
	}

	/**  第一次启动Job Generator */
	private def startFirstTime(): Unit ={
		val startTime = new Time(clock.getTimeMillis())
		//告诉DStreamGraph第一次启动的时间
		graph.start(startTime)
		//启动定期Job生成器
		timer.start(startTime.milliseconds)
		logInfo(s"成功启动JobGenerator，时间：${startTime}")
	}

	/** 根据checkpoint data重启作业生成器 */
	private def restart(): Unit ={
		//若采用manual clock用于测试，设置时间为checkpointed time
		if(clock.isInstanceOf[GManualClock]) {
			val lastTime = ssc.intialCheckpoint.checkpointTime.milliseconds
			val jumpTime = ssc.sc.conf.getLong("spark.streaming.manualClock.jump", 0L)
			clock.asInstanceOf[GManualClock].setTime(lastTime + jumpTime)
		}

		val batchDuration = graph.batchDuration

		//在checkpoint时间和restart时间之间的Batches是因为应用终止的异常情况
		val checkpointTime = ssc.intialCheckpoint.checkpointTime
		val restartTime = new Time(timer.getRestartTime(graph.zeroTime.milliseconds))
		val downTime = checkpointTime.until(restartTime, batchDuration)
		logInfo(s"在应用程序终止到重启之间的有${downTime.size}个Batches：${downTime.mkString(", ")}")

		//在应用故障前尚未处理的批次
		val pendingTimes = ssc.intialCheckpoint.pendingTimes.sorted(Time.ordering)
		logInfo(s"故障前尚未处理的有${pendingTimes.size}个批次：${pendingTimes.size}")

		//重新调度这些没有被处理的批次
		val timesToReschedule = (pendingTimes ++ downTime).filter(_ < restartTime)
  		.distinct.sorted(Time.ordering)

		logInfo(s"总共需要重新调度处理的有${timesToReschedule.size}个批次：${timesToReschedule.mkString(", ")}")

		timesToReschedule.foreach{time =>
			//

			//将接收的blocks分配给该批次
			jobScheduler.receiverTracker.allocateBlocksToBatch(time)
			jobScheduler.submitJobset(JobSet(time, graph.generateJob(time)))
		}

		//重启作业生成器
		timer.start(restartTime.milliseconds)
		logInfo(s"成功重启JobGenerator：${}")
	}

	/** 生成Job并执行当前时刻的checkpoint */
	private def generateJob(time: Time): Unit = {
		//TODO
		SparkEnv.set(ssc.env)

		Try {

			/*
				将当前Batch的数据分配给当前的Job，注意这里是基于metadata数
				据分配！

				如何处理“半条”数据？首先不可能在一个BatchDuration中处理“半
				条”数据的。

				Receiver Supervisor把Receiver接收数据数据存储的时间点为A，
				而Receiver Supervisor把数据发送给Driver汇报数据的元数据的
				时间点为B，ReceiverTracker把数据分配给Batch的时间点为C。

				A\B\C是有时间的不一致的，由哪一个时间决定了数据分配给谁？由C决
				定的，因为在allocateBlocksToBatch中有同步关键字且是Driver
				级别的，即最终数据被划分进哪个Batch是由allocateBlocksToBatch
				决定的，即什么时候获得锁这个数据就决定了在哪个Batch中，但如果
				还没有划入之前的任何Batch的Metadata都会被划入新的Batch中,所
				以数据可能产生在上一个Batch但是可能划分进下一个Batch中，而数
				据的接收一定是一条一条接收所以有半条数据这种情况是不存在的，哪
				个Receiver解析数据不是一条一条的，所以不可能存在半条数据，当
				Receiver只解析半条数据，没解析完，会继续解析，即使解析完了也
				不一定在这个Batch中执行，也可能放入下一个Batch中，只有在
				allocateBlocksToBatch获取锁那一刻才能确定数据在哪一个Batch
				中。

				注意：这个确定的只是元数据，真正数据是在执行时通过InputInfoTracker获取！

			 */
			jobScheduler.receiverTracker.allocateBlocksToBatch(time)
			//使用已分配的BlockRDD生成作业
			graph.generateJob(time)
		} match {
			case Success(jobs) =>
				//前面只是获取元数据，真正数据是在执行时从InputInfoTracker获取
				val streamIdToInputInfos = jobScheduler.inputInfoTracker.getInfo(time)
				////基于当前Batch Interval中的数据以及业务逻辑生成一个JobSet
				jobScheduler.submitJobset(JobSet(time, jobs, streamIdToInputInfos))
			case Failure(e) =>
				jobScheduler.reportError(s"在${time}生成作业时异常}", e)
		}

		//在任务执行完成后，进行checkpoint，采用异步方式不会阻塞作业执行做成
		eventLoop.post(DoCheckpoint(time, clearCheckpointDataLater = false))
	}

	/** 清理DStream的metadata */
	private def clearMetadata(time: Time): Unit = {
		ssc.graph.clearMetadata(time)

		//若checkpoint开启，则在执行checkpoint，否则标记为已处理
		if(shouldChechpoint) {
			eventLoop.post(DoCheckpoint(time, clearCheckpointDataLater = true))
		} else {
			//若没有开启checkpoint，删除已接收的blocks的metadata信息
			val maxRememberDuration = graph.getMaxInputStreamRememberDuration()
			jobScheduler.receiverTracker.cleanupOldBlockAndBatches(time - maxRememberDuration)
			jobScheduler.inputInfoTracker.cleanup(time - maxRememberDuration)
			markBatchFullyProcessed(time)
		}
	}

	/** 指定checkpoint操作 */
	private def doCheckpoint(time: Time, clearCheckpointDataLater: Boolean): Unit = {
		if(shouldChechpoint && (time - graph.zeroTime).isMultipleOf(ssc.checkpointDuration)) {
			logInfo(s"在时刻${time}开始为DStreamGrapg执行checkpointing...")
			ssc.graph.updateCheckpointData(time)
			checkpointWriter.write(new Checkpoint(ssc, time), clearCheckpointDataLater)
		}
	}

	/** 清理DStream的checkpoint数据 */
	private def clearCheckpointData(time: Time): Unit = {
		ssc.graph.clearCheckpointData(time)

		// 所有的checkpoint信息(例如：哪些Batches被处理过)都放在Block
		// Metadata和WAL日志文件中，因此直接删除即可
		val maxRememberDuration = graph.getMaxInputStreamRememberDuration()
		jobScheduler.receiverTracker.cleanupOldBlockAndBatches(time - maxRememberDuration)
		jobScheduler.inputInfoTracker.cleanup(time - maxRememberDuration)
		markBatchFullyProcessed(time)
	}

	private def markBatchFullyProcessed(time: Time): Unit = {
		lastProcessBatch = time
	}

}
