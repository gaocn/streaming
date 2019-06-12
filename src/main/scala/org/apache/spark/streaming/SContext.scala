package org.apache.spark.streaming

import java.io.{InputStream, NotSerializableException}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.SContextState._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.input.{ReceiverInputDStream, SocketInputDStream}
import org.apache.spark.streaming.graph.DSGraph
import org.apache.spark.streaming.scheduler.{JobScheduler, SListener}
import org.apache.spark.streaming.source.SSource
import org.apache.spark.streaming.ui.{SJobProgressListener, STab}
import org.apache.spark.util._
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.reflect.ClassTag;


/**
	* Spark Streaming应用程序的入口
	* 1、可以根据各种数据源创建DStream，然后可以流对其进行transform和
	* action操作；
	* 2、流处理操作编写完成后，可以通过start()启动流处理程序，通过stop()
	* 结束流处理程序，通过awaitTermination()挂起当前线程直到运行完成。
	*/
class SContext(sc_ : SparkContext, cp_ : Checkpoint, batchDuration_ : Duration) extends Logging{

	/**
		* 不指定checkpoint目录
		* @param sparkContext
		* @param duration  micro-batch 时间窗口
		* @return
		*/
	def this(sparkContext: SparkContext, duration: Duration) = {
		this(sparkContext, null, duration)
	}

	/**
		* 根据SparkConf和时间窗口创建SContext实例
		* @param sparkConf
		* @param duration
		* @return
		*/
	def this(sparkConf: SparkConf, duration: Duration) = {
		this(SContext.createSparkContext(sparkConf), null, duration)
	}

	/**
		* 根据指定的详细信息构建SContext实例
		* @param master 例如：local[1]、spark://host1:port1,host2:port2
		* @param appName Streaming应用程序名称
		* @param batchDuration 时间窗口，用于将数据划分为一个个batch
		*                      进行处理
		* @param sparkHome Spark程序目录位置
		* @param jars      指定要加载的jar包
		* @param environment   其他环境参数
		*/
	def this(
						master:String,
						appName:String,
						batchDuration:Duration,
						sparkHome:String=null,
						jars: Seq[String] = Nil,
						environment: Map[String, String] = Map()) {

		this(SContext.createSparkContext(master, appName, sparkHome, jars, environment), null, batchDuration)
	}

	/**
		* 根据checkpoint文件创建SContext实例
		* @param path checkpoint文件夹所在路径
		* @param hadoopConf 可选，若是从HDFS中读取文件，则需要该配置
		* @return
		*/
	def this(path: String, hadoopConf: Configuration) = {
		this(null, CheckpointReader.read(path, new SparkConf(), hadoopConf).get, null)
	}

	/**
		* 根据已有SparkContext创建SContext实例并指定checkpoint路径
		* @param path checkpoint路径
		* @param sparkContext 已有Spark上下文
		* @return
		*/
	def this(path:String, sparkContext: SparkContext) = {
		this(
			sparkContext,
			CheckpointReader.read(path, sparkContext.conf,sparkContext.hadoopConfiguration).get,
			null)
	}

	/**###############   构造函数，初始化   #################*/

	if (sc_ == null && cp_ == null) {
		throw new Exception("当SparkContext和Checkpoint路径均为空时" +
			"无法对SContext进行初始化！")
	}

	val isCheckpointPresent =  (cp_ != null)
	private[streaming] val sc: SparkContext = {
		if (sc_ != null) {
			sc_
		} else if (isCheckpointPresent) {
			SparkContext.getOrCreate(cp_.createSparkConf())
		} else {
			throw new Exception("当SparkContext为空时" +
				"无法对SContext进行初始化！")
		}
	}

	if (sc.conf.get("spark.master") == "local" || sc.conf.get("spark.master") == "local[1]") {
		logWarning("Streaming应用程序本地运行至少需要两个线程，不能使用local或local[1]模式")
	}

	val conf = sc.conf
	val env = sc.env

	//生成InputStream ID
	private[streaming] val nextInputStreamId  = new AtomicInteger(0)

	//设置上下文状态为正在初始化
	private[streaming] var state: SContextState = INITIALIZING

	//用户挂起
	private[streaming] val waiter = new ContextWaiter

	// JVM钩子，用于在上下文退出时被调用
	private[streaming] var shutdownHookRef:  AnyRef = _


	/** DStreamGraph 初始化 */
	private[streaming] val graph:DSGraph = {
		if(isCheckpointPresent) {
			cp_.graph.setContext(this)
			cp_.graph.restoreCheckpointData()
			cp_.graph
		} else {
			require(batchDuration_ != null, "流处理应用必须指定Batch Duration")
			val newGraph = new DSGraph()
			newGraph.setBatchDuration(batchDuration_)
			newGraph
		}
	}

	/** Checkpoint 初始化 */
	private[streaming] var checkpointDir:String = {
		if (isCheckpointPresent) {
			sc.setCheckpointDir(cp_.checkpointDir)
			cp_.checkpointDir
		} else {
			null
		}
	}

	private[streaming] val checkpointDuration = {
		if (isCheckpointPresent) {
			cp_.checkpointDuration
		} else {
			graph.batchDuration
		}
	}

	/** JobScheduler 初始化 */
	private[streaming] val scheduler = new JobScheduler(this)

	/** 作业监听器 初始化 */
	private[streaming] val progressListener = new SJobProgressListener(this)

	/** WebUI 初始化 */
	private[streaming] val uiTab: Option[STab] = {
		if (conf.getBoolean("spark.streaming.ui", true)) {
			Some(new STab(this))
		} else {
			None
		}
	}

	/** 监控指标收集 */
	private[streaming] val streamingSource = new SSource(this)

	/** CallSite */
	//代表用户代码中一个位置
	private[streaming] val startSite = new  AtomicReference[CallSite](null)


	/** 如果用户指定了最新的checkpoint目录，则将使用，若没有指定则使用默认的*/
	conf.getOption("spark.streaming.checkpoint.directory")
  	.foreach(checkpoint)


	/**###############   SContext管理接口   #################*/

	private[streaming] def getStartSite():CallSite = startSite.get()

	/** 返回关联的Spark上下文 */
	def sparkContext: SparkContext = sc

	/**
		*
		* 为了确保Driver应用程序的容错，上下文需要周期性的将DStream操作
		* checkpoint到具有高容错性的文件系统中。
		*
		* @param checkpointDir HDFS-compatible目录，以可靠的方式存储
		*                      checkpoint数据。
		*                      注意：这里的目录最好是支持容错的文件系统。
		*/
	def checkpoint(directory: String): Unit = {
		logInfo(s"spark.streaming.checkpoint.directory指定目录为：${checkpointDir}")
		if(directory != null) {
			val path = new Path(directory)
			val fs = path.getFileSystem(sparkContext.hadoopConfiguration)
			fs.mkdirs(path)
			val fullPath = fs.getFileStatus(path).getPath.toString
			sc.setCheckpointDir(fullPath)
			checkpointDir = fullPath
		} else {
			checkpointDir = null
		}
	}

	private[streaming] def isCheckpointingEnabled: Boolean = {
		checkpointDir != null
	}
	private[streaming] def intialCheckpoint: Checkpoint = {
		if (isCheckpointPresent) cp_ else null
	}

	private[streaming] def getNewInputStreamId() = nextInputStreamId.getAndIncrement()

	/**
		* DStream中生成的RDD会保留一段时间，超时后会释放回收以便重用内存。
		* 该方法可以设置让每个DStream保留RDD为指定duration时长，如果开
		* 发者需要查询以前的在默认DStream计算范围之外的RDD，则可以使用该
		* 方法。
		* @param duration 每个DStream会保留RDD的最小时长
		*/
	def remember(duration: Duration): Unit = {
		graph.remember(duration)
	}

	/**
		* 添加监听器，用于监听与streaming相关的事件
		*/
	def addStreamingListener(sListener: SListener): Unit = {
		scheduler.listenerBus.addListener(sListener)
	}

	private def validate(): Unit = {
		assert(graph != null, "DStreamGraph为空")
		graph.validate()

		require(!isCheckpointingEnabled || checkpointDuration != null,
			"checkpointDir设置，当checkpointDuration没有设置，请调用" +
				"SContext.checkpoint进行设置")

		//检测DStream的checkpint对象是否可序列化
		if(isCheckpointingEnabled) {
			val cpTest = new Checkpoint(this, Time(0))
			try {
				Checkpoint.serialize(cpTest, conf)
			} catch {
				case e: NotSerializableException =>
					throw new NotSerializableException(
						"DStream的checkpoing机制已启用，但DStream及其相关函数" +
							"无法序列化\n" +  e.getMessage)
			}
		}

		if (Utils.isDynamicAllocationEnabled(sc.conf)) {
			logWarning("Streaming应用程序开启了内存动态分配机制，" +
				"若没有开启WAL(Write Ahead Log)，对于像Flume这样无法重放" +
				"源数据的数据源就会导致可能会导致数据丢失。")
		}
	}


	/**
		* 返回当前上下文的状态
		* 	INITIALIZING、ACTIVE、STOPPED
		*/
	def getState(): SContextState = {
		state
	}

	/**
		* 启动DStream，开始执行流处理操作
		* @throws IllegalStateException 若上下文已经停止，则抛出异常
		*/
	def start(): Unit = synchronized{
		state match {
			case INITIALIZING =>
				startSite.set(DStream.getCreationSite())
				//logInfo(s"SContext CallSite值为：${startSite.get()}")
				SContext.ACTIVATION_LOCK.synchronized{
					SContext.assertNoOtherContextIsActive()
					try {
						validate()
						ThreadUtils.runInNewThread("SContext-Starting-Thread"){
							sc.setCallSite(startSite.get())
							sc.clearJobGroup()
							sc.setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "false")
							scheduler.start()
						}
						state = ACTIVE
					} catch {
						case e: Exception =>
							logError(s"启动SContext时出错：${e.getMessage}")
							scheduler.stop(false)
							state = STOPPED
							throw e
					}
					SContext.setActiveContext(this)
				}

				shutdownHookRef  =   ShutdownHookManager
					.addShutdownHook(SContext.SHUTDOWN_HOOK_PRIORITY)(stopOnShutdown)
				assert(env.metricsSystem != null)
				env.metricsSystem.registerSource(streamingSource)
				uiTab.foreach(_.attach())
				logInfo("SContext启动成功")

			case ACTIVE =>

				/**
					* 同一个Streaming应用程序，不能创建多个SContext，因为同
					* 一个Spark应用程序内部不允许有多个SparkContext。
					*/
				logWarning("SContext已经启动")

			case   STOPPED =>
				throw new IllegalStateException("SContext已经停止，无法" +
					"被重用")
		}
	}

	/**
		* 等待执行结束，运行过程中的任何异常都会被抛出
		*/
	def awaitTermination():Unit = {
		waiter.waitForStopOrError()
	}

	/**
		*  等待执行结束，运行过程中的任何异常都会被抛出
		* @param timeout 等待时长，单位毫秒
		* @return true表示停止，否则会抛出异常
		*         false表示超时
		*/
	def awaitTermination(timeout:Long):Boolean = {
		waiter.waitForStopOrError(timeout)
	}



	def stop(stopSparkContext:Boolean = conf.getBoolean("spark.streaming.stopSparkContextByDefault", true)):Unit = synchronized{
		stop(stopSparkContext, false)
	}

	/**
		*
		* @param stopSparkContext true会将关联SparkContext也关闭，
		*                         不管当前SContext是否已启动。
		* @param stopGracefully   true 优雅关闭，等所有接收数据进程
		*                         结束后关闭。
		*/
	def stop(stopSparkContext: Boolean, stopGracefully:Boolean): Unit = {
		var shutdownHookRefToRemove:AnyRef = null
		if(AsynchronousListenerBus.withinListenerThread.value) {
			throw new Exception("无法停止在AsynchronousListenerBus内部线程中的SContext")
		}

		synchronized{
			try {
				state match {
					case INITIALIZING =>
						logWarning("SContext尚未启动")
					case STOPPED =>
						logWarning("SContext已经关闭")
					case ACTIVE =>
						scheduler.stop(stopGracefully)

						//注销MetricsSystem上的SSource
						env.metricsSystem.removeSource(streamingSource)

						//移除WebUI上的 Streaming UITab
						uiTab.foreach(_.detach())

						//清空已启动的SContext实例
						SContext.setActiveContext(null)

						waiter.notifyStop()
						if (shutdownHookRef != null) {
							shutdownHookRefToRemove = shutdownHookRef
							shutdownHookRef = null
						}
						log.info("成功停止SContext！")
				}
			} finally {
				state = STOPPED
			}
		}

		if(shutdownHookRefToRemove != null) {
			ShutdownHookManager.removeShutdownHook(shutdownHookRefToRemove)
		}

		/**
			* 因为用户可能 先调用stop(false)，然后调用stop(true)
			*/
		if(stopSparkContext) {
			sc.stop()
		}
	}

	private def stopOnShutdown() ={
		val stopGracefully = conf
			.getBoolean("spark.streaming.stopGracefullyOnShutdown",false)
		logInfo("")
		//不关闭SparkContext，让其调用自己的ShutdownHook
		stop(false, stopGracefully)
	}

	/**
		* Execute a block of code in a scope such that all new DStreams created in this body will
		* be part of the same scope. For more detail, see the comments in `doCompute`.
		*
		* Note: Return statements are NOT allowed in the given body.
		*/
	private[streaming] def withScope[U](body: => U): U = sparkContext.withScope(body)



	/**###############   DStream输入源接口   #################*/

	/**
		* 创建来自TCP的Socket输入流，接收数据为字节流，通过converter将
		* 字节流转换为T类型的对象。
		* @param host TCP主机名
		* @param port TCP端口号
		* @param converter 将字节流转换为对象
		* @param storageLevel 存储级别
		* @tparam T 输入数据类型
		*/
	def socketStream[T:ClassTag](host:String, port:Int, converter:(InputStream)=>	Iterator[T],storageLevel: StorageLevel):ReceiverInputDStream[T] = {
		new SocketInputDStream[T](this, host, port, converter,storageLevel)
	}



}
object SContext  {
	/**  SContext Shutdown Hook的优先级 */
	private val SHUTDOWN_HOOK_PRIORITY = ShutdownHookManager.SPARK_CONTEXT_SHUTDOWN_PRIORITY  + 1

	/**
		* 若有active(已启动且尚未关闭)的SContext，则重用
		*/
	private val ACTIVATION_LOCK = new Object()
	private val activeContext = new AtomicReference[SContext](null)

	/**  同一个JVM进程只允许有一个SContext实例 */
	private def assertNoOtherContextIsActive(): Unit ={
		ACTIVATION_LOCK.synchronized{
			if (activeContext.get() != null) {
				throw new IllegalStateException("同一个JVM进程内部只能" +
					"有一个SContext实例，目前JVM进程中已有一个正在运行的SContext" +
					s"开始位置为：${activeContext.get().getStartSite().longForm}")
			}
		}
	}

	private def setActiveContext(ssc: SContext): Unit ={
		ACTIVATION_LOCK.synchronized{
			activeContext.set(ssc)
		}
	}

	/**
		* 获取当前已启动且尚未关闭的SContext实例
		*/
	private def getActive():Option[SContext] =  {
		ACTIVATION_LOCK.synchronized{
			Option(activeContext.get())
		}
	}

	/**
		* 要么从执行checkpoint目录下恢复已有的状态后新建SContext，要么
		* 重新创建一个SContext。若checkpointDir存在且里面有可用checkpoint
		* 文件则创建SContext并恢复状态，否则根据`createFunc`重新创建。
		*/
	def getOrCreate(
				 checkpointDir: String,
				 creatingFunc:()=>SContext,
				 hadoopConf: Configuration = SparkHadoopUtil.get.conf,
				 createOnError: Boolean = false): SContext = {
		val checkpointOption = CheckpointReader
			.read(checkpointDir, new SparkConf(), hadoopConf, createOnError)
		checkpointOption
			.map(new SContext(null, _, null))
			.getOrElse(creatingFunc())
	}

	/**
		* 根据指定配置创建SparkContext
 *
		* @param conf
		* @return
		*/
	def createSparkContext(conf: SparkConf): SparkContext = {
		new SparkContext(conf)
	}

	/**
		* 根据指定详细信息创建SparkContext
		*/
	def createSparkContext(
				master: String,
				appName: String,
				sparkHome: String,
				jars: Seq[String],
				environment: Map[String, String]): SparkContext = {
		val conf = SparkContext.updatedConf(new SparkConf(), master, appName, sparkHome, jars, environment)
		new SparkContext(conf)
	}

	private[streaming] def rddToFileName[T](prefix:String, suffix:String, time: Time):String = {
		var result = time.milliseconds.toString
		if (prefix != null && prefix.length > 0) {
			result = s"${prefix}-${result}"
		}
		if (suffix != null && suffix.length > 0) {
			result = s"${result}-${suffix}"
		}
		result
	}
}
