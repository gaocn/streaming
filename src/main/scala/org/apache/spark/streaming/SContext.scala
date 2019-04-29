package org.apache.spark.streaming

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.streaming.SContextState._
import org.apache.spark.streaming.graph.DSGraph
import org.apache.spark.streaming.scheduler.JobScheduler
import org.apache.spark.streaming.source.SSource
import org.apache.spark.streaming.ui.{SJobProgressListener, STab}
import org.apache.spark.util.CallSite;


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

	private val isCheckpointPresent =  (cp_ != null)
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

	if (sc.conf.get("spark.mastere") == "local" || sc.conf.get("spark.master") == "local[1]") {
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
	private[streaming] val checkpointDir:String = {
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


	/**###############   对外提供的接口   #################*/

	private def getStartSite():CallSite = startSite.get()

	/** 返回关联的Spark上下文 */
	def sparkContext: SparkContext = sc



}
object SContext  {

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











}
