package org.apache.spark.streaming.ui

import java.util
import java.util.{Map=>JMap, Properties}

import org.apache.spark.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.streaming.domain.ReceiverInfo
import org.apache.spark.streaming.scheduler._
import org.apache.spark.streaming.{SContext, Time}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

private[streaming] class SJobProgressListener(ssc: SContext) extends SListener with SparkListener with Logging{
	logInfo("创建SJobProgressListener....")
	val batchDuration: Long = ssc.graph.batchDuration.milliseconds
	private val waitingBatchUIData = new mutable.HashMap[Time, BatchUIData]
	private val runningBatchUIData = new mutable.HashMap[Time, BatchUIData]
	private val completedBatchUIData = new mutable.Queue[BatchUIData]
	private val batchUIDataLimit = ssc.conf.getInt("spark.streaming.ui.retainedBatches", 100)
	private var totalCompletedBatches =  0L
	private var totalReceivedRecords = 0L
	private var totalProcessedRecords = 0L
	private val receiverInfos = new mutable.HashMap[Int, ReceiverInfo]

	/**
		* 重写java.utils.LinkedHashMap
		*
		* 由于onJobStart和onBatchXXX消息是在不同线程中处理，因此在接收
		* 到onJobStart事件时，可能无法获取对应的BatchUIData，因此如果采
		* 用Map[Time, BatchUIData]的数据结构有可能会获取空。
		*/
	private val batchTimeToOutputOperationIdSparkJobIdPair = new util.LinkedHashMap[Time, mutable.SynchronizedBuffer[OutputOperationIdAndSparkJobId]]() {
		override def removeEldestEntry(eldest: JMap.Entry[Time, mutable.SynchronizedBuffer[OutputOperationIdAndSparkJobId]]): Boolean = {
			//当大量`onBatchCompleted`发生在`onJobStart`之前时，例如:当
			// sc.listenerBus处理速度很慢时，在`onBatchComplete`移除
			// 某一批次的信息后，`onJobStart`才开始将批次信息添加到
			// batchTimeToOutputOperationIdSparkJobIdPair中，这就会出
			// 现”内存泄露“，因为为了避免这个情况，我们需要控制该队列的长度，
			// 将`最老`的信息给淘汰。
			//
			//而当大量`onJobStart`在`onBatchCompleted`之前发生时，例如：
			//ssc.sListenerBus处理速度慢，又会导致该队列短时间内急剧增加，
			// 可能会操作要保留的批次信息，因此这里添加10作为`削峰`的临时解
			// 决方法，一般情况能够解决大部分问题。
			size() > waitingBatchUIData.size + runningBatchUIData.size + completedBatchUIData.size + 10
		}
	}

	override def onReceiverStarted(receiverStarted: SListenerReceiverStarted): Unit = {
		synchronized{
			receiverInfos(receiverStarted.receiverInfo.streamId)=receiverStarted.receiverInfo
		}
	}

	override def onReceiverStopped(receiverStopped: SListenerReceiverStopped): Unit = {
		synchronized{
			receiverInfos(receiverStopped.receiverInfo.streamId)=receiverStopped.receiverInfo
		}
	}

	override def onReceiverError(receiverError: SListenerReceiverError): Unit = {
		synchronized{
			receiverInfos(receiverError.receiverInfo.streamId)=receiverError.receiverInfo
		}
	}

	override def onBatchSubmitted(batchSubmitted: SListenerBatchSubmitted): Unit = synchronized {
		waitingBatchUIData(batchSubmitted.batchInfo.batchTime)=BatchUIData(batchSubmitted.batchInfo)
	}

	override def onBatchStarted(batchStarted: SListenerBatchStarted): Unit = synchronized{
		val batchUIData = BatchUIData(batchStarted.batchInfo)
		runningBatchUIData(batchStarted.batchInfo.batchTime)= batchUIData
		waitingBatchUIData.remove(batchUIData.batchTime)

		totalReceivedRecords += batchUIData.numTotalRecords
	}

	override def onBatchCompleted(batchCompleted: SListenerBatchCompleted): Unit = synchronized{
		val batchUIData = BatchUIData(batchCompleted.batchInfo)
		waitingBatchUIData.remove(batchCompleted.batchInfo.batchTime)
		runningBatchUIData.remove(batchCompleted.batchInfo.batchTime)
		completedBatchUIData.enqueue(BatchUIData(batchCompleted.batchInfo))

		if(completedBatchUIData.size > batchUIDataLimit) {
			val removeBatch = completedBatchUIData.dequeue()
			batchTimeToOutputOperationIdSparkJobIdPair.remove(removeBatch.batchTime)
		}

		totalCompletedBatches += 1L
		totalProcessedRecords += batchUIData.numTotalRecords
	}

	/**
		* 该方法在onBatchStarted调用后被调用
		*/
	override def onOutputOperationStarted(outputOperationStarted: SListenerOutputOperationStarted): Unit = synchronized{
		runningBatchUIData(outputOperationStarted.outputOperationInfo.batchTime).updateOutputOperationInfo(outputOperationStarted.outputOperationInfo)
	}

	/**
		* 该方法在onBatchCompleted调用后被调用
		*/
	override def onOutputOperationCompleted(outputOperationCompleted: SLitenerOutputOperationCompleted): Unit = {
		runningBatchUIData(outputOperationCompleted.outputOperationInfo.batchTime).updateOutputOperationInfo(outputOperationCompleted.outputOperationInfo)
	}

	override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
		getBatchTimeAndOutputOpId(jobStart.properties).foreach{case(batchTime, outputOpId) =>
			var outputOpIdToSparkJobIds = batchTimeToOutputOperationIdSparkJobIdPair.get(batchTime)
				if (outputOpIdToSparkJobIds == null) {
					outputOpIdToSparkJobIds = new ArrayBuffer[OutputOperationIdAndSparkJobId]() with mutable.SynchronizedBuffer[OutputOperationIdAndSparkJobId]
					batchTimeToOutputOperationIdSparkJobIdPair.put(batchTime, outputOpIdToSparkJobIds)
				}
				outputOpIdToSparkJobIds += OutputOperationIdAndSparkJobId(outputOpId, jobStart.jobId)
		}
	}

	private def getBatchTimeAndOutputOpId(properties:Properties): Option[(Time, Int)] ={
		val batchTime = properties.getProperty(JobScheduler.BATCH_TIME_PROPERTY_KEY)
		if (batchTime == null) {
			None
		} else {
			val outputOpId = properties.getProperty(JobScheduler.OUTPUT_OP_ID_PROPERTY_KEY)
			assert(outputOpId != null)
			Some(Time(batchTime.toLong) -> outputOpId.toInt)
		}

	}

	/******    对外提供接口    *******/
	def numReceivers: Int = synchronized{
		receiverInfos.size
	}

	def numActiveReceivers:Int = synchronized{
		receiverInfos.count(_._2.active)
	}

	def numInactiveReceivers:Int = synchronized{
		ssc.graph.getReceiverInputStreams().size - numActiveReceivers
	}

	def numTotalCompletedBatches: Long = synchronized {
		totalCompletedBatches
	}

	def numTotalReceivedRecords: Long = synchronized {
		totalReceivedRecords
	}

	def numTotalProcessedRecords: Long = synchronized {
		totalProcessedRecords
	}
	def numUnprocessedBatches: Long = synchronized {
		waitingBatchUIData.size + runningBatchUIData.size
	}

	def waitingBatches: Seq[BatchUIData] = synchronized {
		waitingBatchUIData.values.toSeq
	}

	def runningBatches: Seq[BatchUIData] = synchronized {
		runningBatchUIData.values.toSeq
	}

	def retainedCompletedBatches: Seq[BatchUIData] = synchronized {
		completedBatchUIData
	}

	def streamName(streamId: Int): Option[String] = {
		ssc.graph.getInputStreamName(streamId)
	}
	/**
		* 返回所有InputDStream Ids
		*/
	//TODO
	def streamIds: Seq[Int] = {
		//ssc.graph.getInputStreams().map(_.id)
		Seq(1111)
	}

	def receiverInfo(receiverId:Int):Option[ReceiverInfo] = synchronized{
		receiverInfos.get(receiverId)
	}

	/**
		* 返回每个Batch中每个InputDStream中的事件产生速度。
		* key为stream id，value为(BatchTime, EventRate)的序列。
		*/
	def receivedEventRateWithBatchTime:Map[Int, Seq[(Long, Double)]] = synchronized {
		//当前保留的所有批次(waiting+running+completed)的信息
		val _retainedBatches = retainedBatches
		val latestBatches = _retainedBatches.map{batchUIData =>
			(batchUIData.batchTime.milliseconds, batchUIData.streamIdToInputInfo.mapValues(_.numRecords))
		}
		streamIds.map{streamId =>
			val eventRates = latestBatches.map{
				case (batchTime, streamIdToNumRecords) =>
					val numRecords = streamIdToNumRecords.getOrElse(streamId, 0L)
					(batchTime, numRecords * 1000.0 / batchDuration)
			}
			(streamId, eventRates)
		}.toMap
	}

	def lastReceivedBatchRecords:Map[Int, Long] = synchronized{
		lastReceivedBatch
			.map(_.streamIdToInputInfo.mapValues(_.numRecords))
  		.map{lastReceivedBlockInfo =>
				streamIds.map{streamId =>
					(streamId,lastReceivedBlockInfo.getOrElse(streamId, 0L))
				}.toMap
			}
			.getOrElse(streamIds.map(streamId => (streamId, 0L)).toMap)
	}

	def lastCompletedBatch:Option[BatchUIData] =  synchronized{
		completedBatchUIData.sortBy(_.batchTime)(Time.ordering).lastOption
	}

	def lastReceivedBatch:Option[BatchUIData] = synchronized{
		retainedBatches.lastOption
	}

	def retainedBatches:Seq[BatchUIData] =  synchronized{
		(waitingBatchUIData.values.toSeq ++
			runningBatchUIData.values.toSeq ++ completedBatchUIData)
  		.sortBy(_.batchTime)(Time.ordering)
	}

	def getBatchUIData(batchTime:Time):Option[BatchUIData] = synchronized{
		val batchUIData = waitingBatchUIData.get(batchTime).orElse{
			runningBatchUIData.get(batchTime).orElse{
				completedBatchUIData.find(_.batchTime == batchTime)
			}
		}

		batchUIData.foreach{_batchUIData =>
			val outputOpIdToSparkJobIds = batchTimeToOutputOperationIdSparkJobIdPair.get(batchTime)
			if (outputOpIdToSparkJobIds == null) {
				_batchUIData.outputOpIdSparkJobIdPairs = Seq.empty
			} else {
				_batchUIData.outputOpIdSparkJobIdPairs = outputOpIdToSparkJobIds.toArray.asInstanceOf[Seq[OutputOperationIdAndSparkJobId]]
			}
		}
		batchUIData
	}

	/******    测试数据    *******/
	//var numTotalReceivedRecords = 999
	//var numTotalCompletedBatches = 1000
	//val streamName:Map[Int, Option[String]] = Map(1->Some("streamId_1"))

//	val testBatchUIData = Seq(new BatchUIData(Time(System.currentTimeMillis()), Map(1->StreamInputInfo(1, 1000)), System.currentTimeMillis()-30000,Some(System.currentTimeMillis()- 10000),Some(System.currentTimeMillis() - 5000),mutable.HashMap(1->new OutputOperationUIData(2,"op1","已完成",Some(System.currentTimeMillis()-32000),Some(System.currentTimeMillis()-35000),None))))
//
//	runningBatchUIData.put(Time(System.currentTimeMillis()),Seq[BatchInfo](
//		BatchInfo(Time(System.currentTimeMillis()), System.currentTimeMillis()-10000,Some(System.currentTimeMillis()- 9000),Some(System.currentTimeMillis() - 5000),Map(1->StreamInputInfo(1, 1000)), Map(1->OutputOperationInfo(Time(System.currentTimeMillis()-102380), 2,"op1","正在运行",Some(System.currentTimeMillis()-10230),Some(System.currentTimeMillis()-10030),None)))
//	).map(b => BatchUIData(b)).sortBy(_.batchTime.milliseconds).reverse.head)
//
//	waitingBatchUIData.put(Time(System.currentTimeMillis()),Seq[BatchInfo](
//		BatchInfo(Time(System.currentTimeMillis()), System.currentTimeMillis()-1000,Some(System.currentTimeMillis()- 800),Some(System.currentTimeMillis() - 5000),Map(1->StreamInputInfo(1, 1000)), Map(1->OutputOperationInfo(Time(System.currentTimeMillis()-10000), 2,"op1","等待运行",Some(System.currentTimeMillis()-10000),Some(System.currentTimeMillis()-10030),None)))
//	).map(b => BatchUIData(b)).sortBy(_.batchTime.milliseconds).reverse.head)
//
//	completedBatchUIData ++= Seq[BatchInfo](
//		BatchInfo(Time(System.currentTimeMillis()), System.currentTimeMillis()-30000,Some(System.currentTimeMillis()- 10000),Some(System.currentTimeMillis() - 5000),Map(1->StreamInputInfo(1, 1000)), Map(1->OutputOperationInfo(Time(System.currentTimeMillis()-38000), 2,"op1","已完成",Some(System.currentTimeMillis()-32000),Some(System.currentTimeMillis()-35000),None)))
//	).map(b => BatchUIData(b)).sortBy(_.batchTime.milliseconds).reverse

}


object SJobProgressListener {
	type SparkJobId = Int
	type OutputOpId = Int
}