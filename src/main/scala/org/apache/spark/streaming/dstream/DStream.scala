package org.apache.spark.streaming.dstream

import java.io.{IOException, NotSerializableException, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.{BlockRDD, PairRDDFunctions, RDD, RDDOperationScope}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.util.{CallSite, MetadataCleaner}
import org.apache.spark.streaming.SContextState.{ACTIVE, INITIALIZING, STOPPED}
import org.apache.spark.streaming.graph.DSGraph
import org.apache.spark.streaming.scheduler.Job
import org.apache.spark.util.DateTimeUtilis
import org.apache.spark.ui.UIUtils

import scala.collection.mutable.HashMap
import scala.reflect.ClassTag
import scala.util.matching.Regex

/**
	* 离散流(Discretized Stream)用一系列RDD的序列代表持续输入数据流，
	* 离散流的输入可以是TPC套接字、Kafka、Flume等在线实时数据，也可以是
	* 通过对DStream进行transform得到新的。
	*
	* 当该应用程序运行时，每个DStream会周期性地生成一个RDD，要么是根据在
	* 线实时数据要么是通过transform操作产生。
	*
	* 因此DStream有以下特性：
	* 1、当前DStream依赖的其他DStream列表；
	* 2、DStream产生RDD的时间间隔；
	* 3、DStream产生RDD的函数；
	*
	* 当前DStream抽象类中，包含了所有DStream支持的操作，此外对于KV类型
	* 的DStream操作通过隐式转换在[[PairDStreamFunctions]]中提供
	*
	*
	*/
abstract class DStream[T:ClassTag](@transient var ssc: SContext) extends Serializable with Logging{
	validateAtInit()

	//========================================================
	// 子类需要实现的方法
	//========================================================

	/** 在指定时刻生成RDD的方法 */
	def compute(validTime:Time): Option[RDD[T]]

	/** 当前DStream实例的所有父亲DStream列表 */
	def dependencies: List[DStream[_]]

	/** DStream生成RDD的时间间隔 */
	def slideDuration: Duration

	//========================================================
	// 所有DStream通用的方法和成员
	//========================================================

	//添加private[streaming]包可见范围，便于测试
	@transient
	private[streaming] var generatedRDD = new HashMap[Time,RDD[T]]()

	//当前DStream的起始时间
	private[streaming] var zeroTime: Time = null

	//DStream实例保存每个RDD实例的时长
	private[streaming] var rememberDuration: Duration = null

	//DStream中的RDD的存储级别
	private[streaming] var storageLevel: StorageLevel = StorageLevel.NONE

	//checkpoint相关配置
	private[streaming] val mustCheckpoint = false
	private[streaming] var checkpointDuration:Duration = null
	private[streaming] val checkpointData = new DStreamCheckpointData(this)
	@transient
	private var restoredFromCheckpointData = false

	//DStream 的依赖有向无环图
	private[streaming] var graph: DSGraph = null

	//获取当前DStream被创建时的堆栈信息(CallSite)
	private[streaming] val creationSite = DStream.getCreationSite()

	/**
		* 创建当前DStream实例的scope，用于和DStream的操作进行关联。
		*
		* 通过当前scope将DStream实例的操作名(例如：updataStateByKey)
		* 传递给RDDs。需要注意的是我们不会在RDDs中直接使用该scope，而是
		* 基于该scope在每次调用`compute`时创建一个新的scope。
		*
		* 若DStream是不是通过DStream的公共方法创建，则该值为未定义。
		*
		* 参见{@code DStream.makeScope}
		*/
	protected[streaming] val baseScope:Option[String]= {
		val scope = ssc.sc.getLocalProperty(SparkContext.RDD_SCOPE_KEY)
		logInfo(s"当前DSteam实例${this}的baseScope为：${scope}")
		Option(scope)
	}

	private[streaming] def isInialized = (zeroTime != null)

	//要求DStream的所有父亲DStream保存RDD的时间长度
	private[streaming] def parentRememberDuration = rememberDuration

	//返回当前DStream关联的SContext实例
	def context: SContext = ssc

	//用指定存储级别持久化DStream产生的所有RDDs，默认存储级别为内存+序列化
	def persist(sotrageLevel: StorageLevel):DStream[T] = {
		if(this.isInialized) {
			throw new UnsupportedOperationException("SContext已启动，" +
				"无法修改当前DStream的存储级别")
		}
		this.storageLevel = storageLevel
		this
	}
	def persist():DStream[T] = {persist(StorageLevel.MEMORY_ONLY_SER)}
	def cache():DStream[T] = persist

	/**
		* 开启DStream中RDDs的周期性checkpoint机制
		* @param interval 产生的RDD被checkpoint的时间间隔
		*/
	def checkpoint(interval: Duration):DStream[T] = {
		if(this.isInialized) {
			throw new UnsupportedOperationException("SContext已启动，" +
				"无法修改当前DStream的checkpoint interval")
		}
		persist()
		this.checkpointDuration = interval
		this
	}

	private[streaming] def remember(duration: Duration): Unit ={
		if (duration != null &&(rememberDuration == null  || duration > rememberDuration)) {
			rememberDuration = duration
			logInfo(s"设置${this}产生的RDDs的保存周期为：${rememberDuration}")
		}
		dependencies.foreach(_.remember(duration))
	}

	/**********       getOrCompute和generateJob方法       ***********/

	/**
		* 获取某个时刻对应的RDD：1、从缓存中获取；2、重新计算后获取并缓存。
		*/
	private[streaming] final def getOrCompute(time: Time): Option[RDD[T]] =  {
		//1. 若在缓存中则取出
		generatedRDD.get(time).orElse{
			//2.重新计算获取RDD，且time有效(必须是slideDuration的整数倍)
			if(isTimeValid(time)) {
				val rddOption = createRDDWithLocalProperties(time, displayInnerRDDOps=false) {
					PairRDDFunctions.disableOutputSpecValidation.withValue(true){
						compute(time)
					}
				}

				rddOption.foreach{case newRDD =>
					//注册计算出的RDD以便缓存和checkpoint
					if(storageLevel != StorageLevel.NONE) {
						newRDD.persist(storageLevel)
						logInfo(s"持久化${time}时刻RDD(${newRDD.id})，存储级别为${storageLevel}。")
					}

					if(checkpointDuration != null &&(time - zeroTime).isMultipleOf(checkpointDuration)) {
						newRDD.checkpoint()
						logInfo("标记${time}时刻RDD(${newRDD.id})以便进行checkpoint。")
					}
					generatedRDD.put(time, newRDD)
				}
				rddOption
			} else {
				None
			}
		}
	}

	/**
		* Wrap a body of code such that the call site and operation
		* scope information are passed to the RDDs created in this
		* body properly.
		* @param time Current batch time that should be embedded
		*             in the scope names
		* @param body  RDD creation code to execute with certain
		*              local properties.
		* @param displayInnerRDDOps Whether the detailed callsites
		*             and scopes of the inner RDDs generated by `body`
		*             will be displayed in the UI; only the scope and
		*             callsite of the DStream operation that generated
		*             `this` will be displayed.
		*/
	private[streaming] def createRDDWithLocalProperties[U](time:Time, displayInnerRDDOps:Boolean)(body: =>U): U = {
		val scopeKey = SparkContext.RDD_SCOPE_KEY
		val scopeNoOverrideKey = SparkContext.RDD_SCOPE_NO_OVERRIDE_KEY

		// Pass this DStream's operation scope and creation site information to RDDs through
		// thread-local properties in our SparkContext. Since this method may be called from another
		// DStream, we need to temporarily store any old scope and creation site information to
		// restore them later after setting our own.
		val prevCallSite = CallSite(
			ssc.sparkContext.getLocalProperty(CallSite.SHORT_FORM),
			ssc.sparkContext.getLocalProperty(CallSite.LONG_FORM)
		)
		val prevScope = ssc.sparkContext.getLocalProperty(scopeKey)
		val prevScopeNoOverirde = ssc.sparkContext.getLocalProperty(scopeNoOverrideKey)

		try {
			if (displayInnerRDDOps) {
				// Unset the short form call site, so that generated RDDs get their own
				ssc.sc.setLocalProperty(CallSite.SHORT_FORM, null)
				ssc.sc.setLocalProperty(CallSite.LONG_FORM, null)
			} else {
				// Set the callsite, so that the generated RDDs get the DStream's call site and
				// the internal RDD call sites do not get displayed
				ssc.sc.setCallSite(creationSite)
			}

			// Use the DStream's base scope for this RDD so we can (1) preserve the higher level
			// DStream operation name, and (2) share this scope with other DStreams created in the
			// same operation. Disallow nesting so that low-level Spark primitives do not show up.
			// TODO: merge callsites with scopes so we can just reuse the code there
			makeScope(time).foreach{s =>
				ssc.sparkContext.setLocalProperty(scopeKey, s.toJson)
				if (displayInnerRDDOps) {
					// Allow inner RDDs to add inner scopes
					ssc.sparkContext.setLocalProperty(scopeNoOverrideKey, null)
				} else {
					// Do not allow inner RDDs to override the scope set by DStream
					ssc.sparkContext.setLocalProperty(scopeNoOverrideKey, "true")
				}
			}

			body
		} finally {
			// Restore any state that was modified before returning
			ssc.sparkContext.setCallSite(prevCallSite)
			ssc.sparkContext.setLocalProperty(scopeKey, prevScope)
			ssc.sparkContext.setLocalProperty(scopeNoOverrideKey, prevScopeNoOverirde)
		}

	}


	/**
		* 为`time`时刻产生一个Streaming Job，内部调
		* 用方法。 DStream中的默认实现是`time`时刻对应的RDD进行物化。
		*
		* DStream子类可以重载该方法以实现自己的Streaming Job！
		*/
	private[streaming] def generateJob(time: Time):Option[Job] = {
		getOrCompute(time) match {
			case Some(rdd) =>
				val jobFunc = ()=>{
					val emptyFunc = {iter:(Iterator[T])=>{}}
					context.sparkContext.runJob(rdd, emptyFunc)
				}
				Some(new Job(time, jobFunc))
			case None => None
		}
	}

	/**
		* 设置`zeroTime`来初始化当前DStream，以后所有操作都会基于`zeroTime`
		* 基准时间。
		*
		* 该方法会递归调用当前DStream实例的所有父亲DStream！
		*
		* @param time DStream创建时间
		*/
	private[streaming] def initialize(time: Time): Unit = {
		if(zeroTime != null && zeroTime != time) {
			throw new IllegalStateException("当前DStream实例的zeroTime" +
				s"已被初始化为${zeroTime}，无法被再次修改")
		}
		zeroTime = time

		//checkpoint周期不大于10秒且按照slideDuration对齐
		//若slideDuration为3、4、6、7则checkpoint周期为9、8、6、7
		if(mustCheckpoint && checkpointDuration == null) {
			checkpointDuration = slideDuration * math.ceil(Seconds(10)/slideDuration).toInt
			logInfo(s"将checkpoint时间间隔自动设置为：${checkpointDuration}")
		}

		//若rememberDuration未设置，则将其设置为最小值
		var minRememberDuration = slideDuration
		if(checkpointDuration != null && minRememberDuration <= checkpointDuration) {
			//同时至少保存两份checkpoint数据
			minRememberDuration = checkpointDuration * 2
		}

		if(rememberDuration == null || rememberDuration < minRememberDuration) {
			rememberDuration = minRememberDuration
		}

		//递归初始化所有父亲DStream的zeroTime、checkpointDuration、rememberDuration
		dependencies.foreach(_.initialize(time))
	}

	private[streaming] def setContext(s: SContext): Unit = {
		if (ssc != null && ssc != s) {
			throw new IllegalStateException("SContext已被设置，无法重新设置！")
		}
		ssc = s
		logInfo(s"设置SContext上下文：${s}")
		dependencies.foreach(_.setContext(s))
	}

	private[streaming] def setGraph(g: DSGraph): Unit = {
		if(graph != null && graph != g) {
			throw new IllegalStateException("DSGraph已被设置，无法重新设置！")
		}
		graph = g
		logInfo(s"设置DSGraph：${g}")
		dependencies.foreach(_.setGraph(g))
	}

	/** 判断给定`time`是否是有效的slideDuration，以便用于生成RDD */
	private[streaming] def isTimeValid(time: Time):Boolean = {
		if(!isInialized) {
			throw new Exception(s"${this}尚未被初始化。")
		}else if(time <= zeroTime || !(time - zeroTime).isMultipleOf(slideDuration)){
			logInfo("time=${time}，不满足(time-zeroTime)为slideDuration的整数倍，因此无效。")
			false
		} else {
			logInfo(s"time=${time}是有效的。")
			true
		}
	}

	/**
		* 创建scope用于对同一个Batch中同一个DStream实例内部产生的RDD分组
		*
		* 每个DStream会产生多个scope，每个scope可以被在同一操作中创建的
		* 其他DStream共享。对同一个DStream的相同操作的多次调用会产生多个
		* scope，例如：`dstream.map().map()`会分别创建一个scope。
		*/
	private def makeScope(time: Time):Option[RDDOperationScope] = {
		baseScope.map{bsJson =>
			val formattedBatchTime = DateTimeUtilis.formatBatchTime(time.milliseconds,
				ssc.graph.batchDuration.milliseconds, showYYYYMMSS = false)

			val bs = RDDOperationScope.fromJson(bsJson)
			val baseName = bs.name
			val scopeName = {
				if(baseName.length > 10) {
					//操作名太长，换行显示
					s"${baseName}\n@ ${formattedBatchTime}"
				} else {
					s"${baseName}@ ${formattedBatchTime}"
				}
			}

			val scopeId = s"${bs.id}_${time.milliseconds}"
			new RDDOperationScope(scopeName, id = scopeId)
		}
	}

	/** 必须在SContext初始化状态时才能创建DStream实例 */
	private def validateAtInit(): Unit ={
		ssc.getState() match {
			case INITIALIZING =>
				//可以正常进行
			case ACTIVE =>
				val info = "SContext已启动，无法添加新的输入、转换或输出超过"
				logError(info)
				throw new IllegalStateException(info)
			case STOPPED =>
				val info = "SContext已关闭，无法添加新的输入、转换或输出超过"
				logError(info)
				throw new IllegalStateException(info)
		}
	}

	private[streaming] def validateAtStart(): Unit = {
		require(rememberDuration != null, "未设置rememberDuration。")

		require(
			!mustCheckpoint || checkpointDuration != null,
			s"未设置${this.getClass.getSimpleName}的checkpoint周期，" +
				s"调用DStream.checkpoint()来设置checkpoint周期。"
		)

		require(
			checkpointDuration == null || context.sparkContext.checkpointDir.isDefined,
			"未设置checkpoint目录。"
		)

		require(
			checkpointDuration == null || checkpointDuration >= slideDuration,
			s"${this.getClass.getSimpleName}的checkpoint周期(${checkpointDuration})" +
				s"小于RDD生成周期slideDuration(${slideDuration})，checkpoint" +
				s"周期最小为slideDuartion!"
		)

		require(
			checkpointDuration == null || checkpointDuration.isMultipleOf(slideDuration),
			s"${this.getClass.getSimpleName}的checkpoint周期(${checkpointDuration})必须要为RDD生成周期(${slideDuration})的整数倍。"
		)

		require(
			checkpointDuration == null || storageLevel != StorageLevel.NONE,
			s"${this.getClass.getSimpleName}的checkpoint周期已设置，" +
				s"但没有设置存储级别，请调用DStream.persist()进行设置。"
		)

		require(
			checkpointDuration == null || rememberDuration > checkpointDuration,
			"The remember duration for " + this.getClass.getSimpleName + " has been set to " +
				rememberDuration + " which is not more than the checkpoint interval (" +
				checkpointDuration + "). Please set it to higher than " + checkpointDuration + "."
		)

		val metadataCleanerDelay = MetadataCleaner.getDelaySeconds(ssc.conf)
		logInfo(s"${this.getClass.getSimpleName}元数据请求延迟为：" + metadataCleanerDelay)
		require(
			metadataCleanerDelay < 0 || rememberDuration.milliseconds < metadataCleanerDelay * 1000,
			s"当前${this.getClass.getSimpleName}保存产生RDDs的时间为${ rememberDuration.milliseconds / 1000}s，而Spark元数据的清理延迟为${metadataCleanerDelay}s，需要将清理延迟设置为至少${math.ceil(rememberDuration.milliseconds / 1000.0).toInt}s。"
		)

		//递归调用父亲DStream进行验证和启动
		dependencies.foreach(_.validateAtStart())

		logInfo(s"DStream的RDDs产生周期slideDuration为：${slideDuration}")
		logInfo(s"DStream的RDDs存储级别为：${storageLevel}")
		logInfo(s"Checkpoint周期为：${checkpointDuration}")
		logInfo(s"DStream的RDDs产生保存周期为：${rememberDuration}")
		logInfo(s"${this}已完成初始化和校验!")
	}


	/***********    checkpoint和元数据清理方法   **************/

	/**
		* 清理当前DStream中比`rememberDuration`旧的元数据。
		* DStream中的默认实现是清除产生的过期RDDs，DStream的子类可以覆
		* 写该方法以实现自己元数据的清理。
		*/
	private[streaming] def clearMetadata(time: Time):Unit = {
		val unpersistedRDD = ssc.conf.getBoolean("spark.streaming.unpersist", true)
		val oldRdds = generatedRDD
			.filter(_._1 <= (time-rememberDuration))
		logInfo(s"清理对这些RDD的引用：${oldRdds.map(x=>x._1 -> x._2.id).mkString("，")}")
		generatedRDD --= oldRdds.keys

		if(unpersistedRDD) {
			logInfo(s"开始请求RDD的数据：${oldRdds.values.map(_.id).mkString("，")}")
			oldRdds.values.foreach{rdd=>
				rdd.unpersist(false)
				//显示移除RDD的blocks
				rdd match {
					case b: BlockRDD[_] =>
						logInfo(s"移除${time}时刻的RDD blocks")
						b.removeBlocks()
					case _ =>
				}
			}
		}
		logInfo(s"清理${oldRdds.size}个在${time-rememberDuration}之前的RDDs: ${oldRdds.keys.mkString("，")}")
		dependencies.foreach(_.clearMetadata(time))
	}

	/**
		* 刷新需要进行checkpointed的RDDs列表。
		*
		* DStream中的默认实现是只保存checkpointed RDD的文件名到
		* CheckpointData中。DStream子类可以覆写该方法以实现保存自定义
		* CheckpointDat。
		*/
	private[streaming] def updateCheckpointData(currentTime:Time): Unit ={
		logInfo(s"更新需要进行checkpointed的RDDs列表，当前时间为：${currentTime}")
		checkpointData.update(currentTime)
		dependencies.foreach(_.updateCheckpointData(currentTime))
		logInfo(s"更新需要进行checkpointed的RDDs列表，当前时间为：${currentTime}，更新后的数据为：${checkpointData}")
	}

	private[streaming] def clearCheckpointData(time: Time): Unit = {
		logDebug("清理CheckpointData")
		checkpointData.clearup(time)
		dependencies.foreach(_.clearCheckpointData(time))
		logDebug("清理CheckpointData")
	}

	/**
		* 从CheckpointData中恢复`generatedRDD`.
		*
		* DStream中的默认实现是根据保存的checkpoint文件名恢复RDDs。
		* CheckpointData中。DStream子类可以覆写该方法时同时需要覆写方法
		* `updateCheckpointData()`。
		*/
	private[streaming] def restoreCheckpointData(): Unit = {
		if(!restoredFromCheckpointData) {
			logDebug("从CheckpointData中恢复保存的RDDs")
			checkpointData.restore()
			dependencies.foreach(_.restoreCheckpointData())
			logDebug("从CheckpointData中恢复保存的RDDs")
			restoredFromCheckpointData = true
		}
	}

	private[streaming] def writeObject(oos:ObjectOutputStream): Unit = {
		try {
			logInfo(s"${this.getClass.getSimpleName}.writeObject方法被调用")
			if (ssc.graph != null) {
				ssc.graph.synchronized {
					if (ssc.graph.checkpointInProgress) {
						oos.defaultWriteObject()
					} else {
						val msg = s"需要将${this.getClass.getSimpleName}对象作为RDD" +
							s"操作闭包的一部分被序列化，因为DStream实例在闭包内被引用。因此需" +
							s"要覆写当前DStream实例的RDD操作以避免该异常。通过当前操作可以避免Spark" +
							s"任务中一些无用的对象占用有限的内存资源。"
						throw new NotSerializableException(msg)
					}
				}
			} else {
				throw new NotSerializableException("DStream序列化时，对应的DStreamGraph实例为空！")
			}
		} catch {
			case e:IOException =>
				logError(s"序列化异常：${e.getMessage}")
				throw e
			case e: Exception  =>
				logError(s"序列化异常：${e.getMessage}")
				throw new IOException(e.getMessage)
		}
	}

	private[streaming] def readObject(ois:ObjectInputStream):Unit = {
		try {
			logInfo(s"${this.getClass.getSimpleName}.readObject方法被调用")
			ois.defaultReadObject()
			generatedRDD = new HashMap[Time, RDD[T]]()
		} catch {
			case e:IOException =>
				logError(s"反序列化异常：${e.getMessage}")
				throw e
			case e: Exception  =>
				logError(s"反序列化异常：${e.getMessage}")
				throw new IOException(e.getMessage)
		}
	}

	// =======================================================================
	// DStream operations
	// =======================================================================

	def map[U:ClassTag](mapFunc: T=>U):DStream[U] = ssc.withScope {
		new MappedDStream(this, context.sparkContext.clean(mapFunc))
	}

	def flapMap[U:ClassTag](flatMapFunc: T=>Traversable[U]) = ssc.withScope{
		new FlatMappedDStream(this, context.sparkContext.clean(flatMapFunc))
	}

	def filter(filterFunc: T=>Boolean):DStream[T] = ssc.withScope{
		new FilteredDStream(this, context.sparkContext.clean(filterFunc))
	}

	//创建新的DStream实例，并对其中的所有RDDs调用glom方法，针对每个RDD，
	// glom会将该RDD中的每个分区中的数据放在数组而不是在迭代器中
	def glom():DStream[Array[T]] = ssc.withScope{
		new GlommedDStream(this)
	}

	//与另一DStream进行合并，要求两个DStream有相同的slideDuration！！
	def union(that:DStream[T]):DStream[T] = ssc.withScope{
		new UnionDStream[T](Array(this, that))
	}

	//创建新的DStream，该DStream中的每个RDD会应用一个函数
	def transform[U:ClassTag](transformFunc: (RDD[T], Time)=>RDD[U]):DStream[U] = ssc.withScope{
		//
		val cleanedFunc = ssc.sparkContext.clean(transformFunc, false)
		val realTransformFunc = (rdds: Seq[RDD[_]], time:Time) => {
			assert(rdds.length == 1)
			cleanedFunc(rdds.head.asInstanceOf[RDD[T]],  time)
		}
		new TransformedDStream[U](Seq(this), realTransformFunc)
	}

	def transform[U:ClassTag](transformFunc: (RDD[T])=>RDD[U]):DStream[U] = ssc.withScope{
		val cleanFunc = context.sparkContext.clean(transformFunc)
		transform((r:RDD[T], t:Time)=> cleanFunc(r))
	}

	def repartition(numPartitions:Int):DStream[T] = ssc.withScope{
		this.transform(_.repartition(numPartitions))
	}

	def slice(interval: Interval):Seq[RDD[T]] = ssc.withScope{
		slice(interval.beginTime, interval.endTime)
	}

	/**
		* 返回从`fromTime`到`endTime`之间的RDDs(包含)
		*/
	def slice(fromTime: Time, toTime:Time):Seq[RDD[T]] = ssc.withScope{
		if(!isInialized) {
			throw new IllegalStateException(s"${this}尚未初始化完成！")
		}

		val alignedToTime = if((toTime-zeroTime).isMultipleOf(slideDuration)) {
			toTime
		} else {
			logWarning(s"toTime(${toTime})不是slideDuration${slideDuration}的" +
				s"整数倍，调整为按照slideDuration对齐！")
			toTime.floor(slideDuration)
		}

		val alignedFromTime = if((fromTime-zeroTime).isMultipleOf(slideDuration)) {
			fromTime
		} else {
			logWarning(s"fromTime(${fromTime})不是slideDuration${slideDuration}的" +
				s"整数倍，调整为按照slideDuration对齐！")
			fromTime.floor(slideDuration)
		}

		logInfo(s"时间范围[${fromTime}，${toTime}]，按照slideDuration(${slideDuration})对齐为[${alignedFromTime}，${alignedToTime}]")

		alignedFromTime.to(alignedToTime,slideDuration).flatMap(time=>{
			if(time > zeroTime) {
				getOrCompute(time)
			}  else {
				None
			}
		})
	}

	/***************** window function操作  ********************/

	def window(windowDuration: Duration):DStream[T] = {
		window(windowDuration, this.slideDuration)
	}

	/**
		* 新建DStream实例，其功能是确保每个RDDs中包含窗口大小范围内的元素
		* 能够被访问到，并且会随着活动窗口在动态变化！
		* @param windowDuration 窗口长度，必须为DStream的BatchInterval
		*                       的整数倍。
		* @param slideDuration  滑动窗口，必须为DStream的BatchInterval
		*                       的整数倍。
		* @return
		*/
	def window(windowDuration: Duration, slideDuration:Duration):DStream[T] = {
		new WindowDStream(this, windowDuration,slideDuration)
	}


	/***************** Action及输出流操作  ********************/

	def reduce(reduceFunc:(T,T)=>T):DStream[T] = ???
	def count():DStream[Long] = ???
	def countByValue(numPartitions: Int = ssc.sc.defaultParallelism)(implicit ord:Ordering[T]=null):DStream[(T, Long)] = ???

	/**
		* DStream的子类中只有ForEachDStream复写了DStream的generateJob
		* 方法！！也就是说最后作业的生成，是由JobGenerator调用
		* ForEachDStream的generateJob。
		*/
	def foreachRDD(foreachFunc:(RDD[T],Time)=>Unit, displayInnerRDDOps:Boolean):Unit = ssc.withScope{
		new ForEachDStream(this, context.sparkContext.clean(foreachFunc, false), displayInnerRDDOps).register()
	}

	def print():Unit = ssc.withScope{
		print(10)
	}

	def print(num:Int): Unit = ssc.withScope{
		//最后是调用action进行RDD作业触发，但这里是函数并不会立刻调用，
		//而是会通过JobScheduler通过线程池的方式执行，即定义和执行分离！
		def foreachFunc: (RDD[T], Time) => Unit = {
			(rdd:RDD[T], time:Time)=>{
				val firstNum = rdd.take(num + 1)
				println("------------------------------------")
				println(s"时间：${time}")
				println("------------------------------------")
				firstNum.take(num).foreach(println)
				if(firstNum.length > num)println("...")
				println()
			}
		}
		foreachRDD(context.sparkContext.clean(foreachFunc),false)
	}

	def saveAsTextFile(prefix:String, suffix:String = ""):Unit = ssc.withScope{
		val saveFunc = (rdd:RDD[T],time:Time)=>{
			val file = SContext.rddToFileName(prefix,suffix,time)
			rdd.saveAsTextFile(file)
		}

		this.foreachRDD(saveFunc, false)
	}

	def saveAsObjectFile(prefix:String, suffix:String = ""):Unit = ssc.withScope{
		val saveFunc = (rdd:RDD[T],time:Time)=>{
			val file = SContext.rddToFileName(prefix,suffix,time)
			rdd.saveAsObjectFile(file)
		}

		this.foreachRDD(saveFunc, false)
	}

	/**
		* 将输出流注册到
		*/
	private[streaming] def register():DStream[T] = {
		ssc.graph.addOutputStream(this)
		this
	}

}

object DStream {
	/** Get the creation site of a DStream from the stack trace of when the DStream is created. */
	private[streaming] def getCreationSite(): CallSite = {
		val SPARK_CLASS_REGEX = """^org\.apache\.spark""".r
		val SPARK_STREAMING_TESTCLASS_REGEX = """^org\.apache\.spark\.streaming\.test""".r
		val SPARK_EXAMPLES_CLASS_REGEX = """^org\.apache\.spark\.examples""".r
		val SCALA_CLASS_REGEX = """^scala""".r

		/** Filtering function that excludes non-user classes for a streaming application */
		def streamingExclustionFunction(className: String): Boolean = {
			def doesMatch(r: Regex): Boolean = r.findFirstIn(className).isDefined
			val isSparkClass = doesMatch(SPARK_CLASS_REGEX)
			val isSparkExampleClass = doesMatch(SPARK_EXAMPLES_CLASS_REGEX)
			val isSparkStreamingTestClass = doesMatch(SPARK_STREAMING_TESTCLASS_REGEX)
			val isScalaClass = doesMatch(SCALA_CLASS_REGEX)

			// If the class is a spark example class or a streaming test class then it is considered
			// as a streaming application class and don't exclude. Otherwise, exclude any
			// non-Spark and non-Scala class, as the rest would streaming application classes.
			(isSparkClass || isScalaClass) && !isSparkExampleClass && !isSparkStreamingTestClass
		}
		org.apache.spark.util.Utils.getCallSite(streamingExclustionFunction)
	}









}
