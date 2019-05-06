package org.apache.spark.streaming.scheduler

import org.apache.spark.streaming.domain.{BatchInfo, OutputOperationInfo, ReceiverInfo}
import org.apache.spark.util.Distribution

import scala.collection.mutable


sealed trait SListenerEvent
case class SListenerBatchStarted(batchInfo: BatchInfo) extends SListenerEvent
case class SListenerBatchSubmitted(batchInfo: BatchInfo) extends SListenerEvent
case class SListenerBatchCompleted(batchInfo: BatchInfo) extends SListenerEvent

case class SListenerOutputOperationStarted(outputOperationInfo: OutputOperationInfo) extends SListenerEvent
case class SLitenerOutputOperationCompleted(outputOperationInfo: OutputOperationInfo) extends SListenerEvent

case class SListenerReceiverStarted(receiverInfo: ReceiverInfo) extends SListenerEvent
case class SListenerReceiverStopped(receiverInfo: ReceiverInfo) extends SListenerEvent
case class SListenerReceiverError(receiverInfo: ReceiverInfo) extends SListenerEvent

private[streaming] trait SListener {

	def onBatchStarted(batchStarted: SListenerBatchStarted){}
	def onBatchSubmitted(batchSubmitted: SListenerBatchSubmitted){}
	def onBatchCompleted(batchCompleted: SListenerBatchCompleted){}

	def onOutputOperationStarted(outputOperationStarted: SListenerOutputOperationStarted){}
	def onOutputOperationCompleted(outputOperationCompleted: SLitenerOutputOperationCompleted){}

	def onReceiverStarted(receiverStarted: SListenerReceiverStarted){}
	def onReceiverStopped(receiverStopped: SListenerReceiverStopped){}
	def onReceiverError(receiverError: SListenerReceiverError){}
}

/**
	* 用于打印多个batches的统计信息
	* @param numBatchInfos
	*/
class StatsReportListener(numBatchInfos: Int = 10) extends SListener {
	val batchInfos = new mutable.Queue[BatchInfo]()

	override def onBatchCompleted(batchCompleted: SListenerBatchCompleted): Unit = {
		batchInfos.enqueue(batchCompleted.batchInfo)
		if (batchInfos.size > numBatchInfos) batchInfos.dequeue()
		printStats()
	}

	def printStats(): Unit = {
		showMillisDistribution(s"${numBatchInfos}个batch的平均总延迟为：", _.totalDelay)
		showMillisDistribution(s"${numBatchInfos}个batch的平均处理时间为：", _.processingDelay)
	}

	def showMillisDistribution(heading: String, getMetric: BatchInfo => Option[Long]): Unit = {
		org.apache.spark.scheduler.StatsReportListener.showMillisDistribution(heading, extractDistribution(getMetric))
	}

	def extractDistribution(getMetric: BatchInfo => Option[Long]): Option[Distribution] = {
		Distribution(batchInfos.flatMap(getMetric(_)).map(_.toDouble))
	}
}