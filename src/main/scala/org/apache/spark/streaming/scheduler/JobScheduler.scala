package org.apache.spark.streaming.scheduler

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import org.apache.spark.Logging
import org.apache.spark.streaming.{SContext, Time}
import org.apache.spark.util.{EventLoop, ThreadUtils}

import scala.collection.JavaConverters._
import scala.util.Failure
import JobScheduler._
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.ui.UIUtils

sealed trait JobSchedulerEvent
case class JobStarted(job:Job, startTime: Long) extends JobSchedulerEvent
case class JobCompleted(job: Job, completedTime: Long) extends JobSchedulerEvent
case class ErrorReported(msg: String, e: Throwable) extends JobSchedulerEvent


/**
	* [[JobScheduler]]负责将产生的作业提交给Spark中运行，它通过[[JobGenerator]]
	* 产生作业，并在线程池中提交运行这些作业。
	*
	* [[JobScheduler]]是Spark Streaming作业的调度者，是完成Spark
	* Streaming和Spark Core衔接的控制点，即在Spark Streaming生成作
	* 业后由[[JobScheduler]]交给Spark Core执行。
	*
	* Spark Core中生成作业的是SparkContext，而SparkStreaming中生成
	* 作业的是[[JobGenerator]]。[[JobGenerator]]是根据RDD生成作业，
	* 而RDD中是有数据来源的，这就需要BlockManager管理器用于获取操作数
	* 据的元数据，Spark Streaming作为上层抽象框架，提出了[[ReceiverTracker]]
	* 底层封装了BlockManager。
	*
	*/
private[streaming]
class JobScheduler(val ssc: SContext) extends Logging{
	logInfo("创建JobScheduler")

	// Use of ConcurrentHashMap.keySet later causes an odd
	// runtime problem due to Java 7/8 diff
	// https://gist.github.com/AlainODea/1375759b8720a3f9f094
	//每个Batch Duration对应生成的作业，因为输出可能有多个，所以生成的
	// 作业就会多个
	private val jobSets: java.util.Map[Time, JobSet] = new ConcurrentHashMap[Time, JobSet]()

	/*
		从Spark Streaming Context中获取并发作业的配置，在Spark Core上
		可以同时运行多个作业，采用多线程实现，每个Job用一个单独线程运行！
		超过并发作业数时会进入队列中，Job按照FIFO进行调度。

		Spark Core的FIFO调度策略是应用程序级别的！这里是同一个应用程序内
		部的多个Job并发运行！

		默认情况下没有必要该并发度，为什么有时候需要修改并发度？有时候一个
		应用程序有多个输出，会有多个Job的执行，由于在同一个Batch Duration
		中各自Job之间没有必要相互等待，因此可以调整作业并发度。

	 */
	private val numConcurrentJobs = ssc.conf.getInt("spark.streaming.concurrentJobs", 1)
	private val jobExecutor = ThreadUtils.newDaemonFixedThreadPool(numConcurrentJobs, "streaming-job-executor")

	private val jobGenerator = new JobGenerator(this)
	val clock = jobGenerator.clock

	/** 这两个tracker在job scheduler启动时才创建，若eventLoop不为空
		* 则说明已经启动 */
	var receiverTracker:ReceiverTracker = null

	/** 记录所有输入流及处理记录条数信息 */
	var inputInfoTracker: InputInfoTracker = null

	//Spark Streaming监控作业的工具
	val listenerBus = new SListenerBus()

	private var eventLoop: EventLoop[JobSchedulerEvent] = null

	/*================
	 * 对外暴露的方法
	 *================
	 */
	def start(): Unit = synchronized{
		logInfo("启动JobScheduler调度器....")
		if(eventLoop != null) return

		//EventLoop：将调度和业务解耦合，代码架构和可维护性增加
		eventLoop = new EventLoop[JobSchedulerEvent]("JobScheduler") {
			override protected def onReceive(event: JobSchedulerEvent): Unit = processEvent(event)
			override protected def onError(e: Throwable): Unit = reportError("Job Scheduler出错", e)
		}
		eventLoop.start()

		//将InputStreams的RateController与每次Batch执行完成后的更新操作绑定
		for{
			inputDStream <- ssc.graph.getInputStreams()
			rateController <- inputDStream.rateController
		} ssc.addStreamingListener(rateController)

		listenerBus.start(ssc.sparkContext)
		receiverTracker = new ReceiverTracker(ssc)
		inputInfoTracker = new InputInfoTracker(ssc)
		receiverTracker.start()
		jobGenerator.start()
		logInfo("成功启动JobSScheduler")
	}

	def stop(processAllReceivedData:Boolean): Unit = synchronized{
		logInfo("停止JobScheduler调度器....")

		//scheduler已经被停止
		if(eventLoop == null) return

		if(receiverTracker != null) {
			//1、停止ReceiverTracker
			receiverTracker.stop(processAllReceivedData)
		}

		//2、停止JobGenerator，若需要在处理完所有接收数据后关闭，则会阻塞
		jobGenerator.stop(processAllReceivedData)

		//3、停止线程池
		logInfo("停止线程池......")
		jobExecutor.shutdown()

		//若为优雅停止，则等待线程池队列中作业处理完后返回
		val terminated = if(processAllReceivedData) {
			jobExecutor.awaitTermination(1, TimeUnit.HOURS)
		} else {
			jobExecutor.awaitTermination(2, TimeUnit.SECONDS)
		}

		if(!terminated) {
			jobExecutor.shutdownNow()
		}
		logInfo("成功停止线程池")

		listenerBus.stop()
		eventLoop.stop()
		eventLoop = null
		logInfo("成功停止JobScheduler")
	}

	def submitJobset(jobSet: JobSet): Unit = {
		if (jobSet.jobs.isEmpty) {
			/*
				什么时候没有Job？没有数据时也会生成Job，除非graph.generateJob
				生成失败，注意这个Job不是Spark Core的Job只是一个Spark Streaming
				中的作业的抽象！
			 */
			logInfo(s"${jobSet.time}时刻，没有要提交个作业")
		} else {
			listenerBus.post(SListenerBatchSubmitted(jobSet.toBatchInfo))
			jobSets.put(jobSet.time, jobSet)

			jobSet.jobs.foreach(job => jobExecutor.execute(new JobHandler(job)))
			logInfo(s"在${jobSet.time}时刻添加作业：${jobSet.jobs}")
		}
	}

	def getPendingTimes():Seq[Time] = {
		jobSets.asScala.keys.toSeq
	}

	def reportError(msg:String, e: Throwable): Unit = {
		eventLoop.post(ErrorReported(msg, e))
	}

	def isStarted: Boolean = synchronized{
		eventLoop != null
	}

	/*
	 * Event Loop Process
	 */
	private def processEvent(event:JobSchedulerEvent): Unit = {
		try {
			event match {
				case JobStarted(job, startTime) =>
					handleJobStart(job, startTime)
				case JobCompleted(job, completedTime) =>
					handleJobCompletion(job, completedTime)
				case ErrorReported(msg, e) =>
					handleError(msg, e)
			}
		} catch {
			case e: Throwable =>
				reportError("Job Scheduler出错", e)
		}
	}

	private def handleJobStart(job: Job, startTime: Long): Unit = {
		val jobSet = jobSets.get(job.time)
		val isFirstJobOfJobset = !jobSet.hasStarted
		jobSet.handleJobStart(job)

		if(isFirstJobOfJobset) {
			//为了正确设置jobSet.processingStartTime时间，需要在第一个job
			// 开始时更新
			listenerBus.post(SListenerBatchStarted(jobSet.toBatchInfo))
		}

		job.setStartTime(startTime)
		listenerBus.post(SListenerOutputOperationStarted(job.toOutputOperationInfo))
		logInfo(s"开始执行${jobSet.time}时刻JobSet中的${job.id}作业")
	}

	private def handleJobCompletion(job: Job, completedTime: Long): Unit = {
		val jobSet = jobSets.get(job.time)
		jobSet.handleJobCompletion(job)
		job.setEndTime(completedTime)
		listenerBus.post(SLitenerOutputOperationCompleted(job.toOutputOperationInfo))
		logInfo(s"执行完成${jobSet.time}时刻JobSet中的${job.id}作业")

		if(jobSet.hasCompleted) {
			jobSets.remove(jobSet.time)
			jobGenerator.onBatchCompletion(jobSet.time)
			logInfo(s"${jobSet.time.toString}时刻的JobSet执行完成：总延迟 %.3f s，处理延迟 %.3f s".format(
				jobSet.totalDelay / 1000.0,
				jobSet.processingDelay / 1000.0
			))
			listenerBus.post(SListenerBatchCompleted(jobSet.toBatchInfo))
		}

		job.result match {
			case Failure(e) =>
				reportError(s"执行作业${job}时出错", e)
			case _ =>
		}
	}

	private def handleError(msg: String, e: Throwable): Unit = {
		logError(msg, e)
		ssc.waiter.notifyError(e)
	}

	private class JobHandler(job: Job) extends Runnable with Logging {
		override def run(): Unit = {
			try {
				val formattedTime = UIUtils.formatDuration(job.time.milliseconds)

				val batchUrl = s"streaming/batch/?id=${job.time.milliseconds}"
				val batchLinkText = s"[output operation ${job.outputOpId}. batch time  ${formattedTime}]"

				ssc.sc.setJobDescription(s"""Streaming job from <a href="${batchUrl}">${batchLinkText}</a>""")
				ssc.sc.setLocalProperty(BATCH_TIME_PROPERTY_KEY, job.time.milliseconds.toString)
				ssc.sc.setLocalProperty(OUTPUT_OP_ID_PROPERTY_KEY, job.outputOpId.toString)

				/*
					将`eventLoop`赋值给临时变量，否则，若当前方法正在运行时JobScheduler.stop(false)
					的调用将`eventLoop`设置为null，导致调用post方法时为null
				 */
				var _eventLoop = eventLoop
				if (_eventLoop != null) {
					_eventLoop.post(JobStarted(job, clock.getTimeMillis()))

					//Disable checks for existing output directories in
					// jobs launched by the streaming  scheduler, since
					// we may need to write output to an existing directory
					// during checkpoint recovery; see SPARK-4835 for more details.
					PairRDDFunctions.disableOutputSpecValidation.withValue(true) {
						job.run()
					}
					_eventLoop = eventLoop
					if (_eventLoop != null) {
						_eventLoop.post(JobCompleted(job, clock.getTimeMillis()))
					}
				} else {
					// job scheduler 已停止
				}
			} finally {
				ssc.sc.setLocalProperty(BATCH_TIME_PROPERTY_KEY, null)
				ssc.sc.setLocalProperty(OUTPUT_OP_ID_PROPERTY_KEY, null)
			}
		}
	}
}

private[streaming] object JobScheduler {
	val BATCH_TIME_PROPERTY_KEY = "spark.streaming.internal.batchTime"
	val OUTPUT_OP_ID_PROPERTY_KEY = "spark.streaming.internal.outputOpId"
}