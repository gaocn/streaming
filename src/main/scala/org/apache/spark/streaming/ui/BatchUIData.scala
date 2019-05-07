package org.apache.spark.streaming.ui

import SJobProgressListener._
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.domain.{BatchInfo, OutputOperationInfo, StreamInputInfo}

import scala.collection.mutable

case class OutputOperationIdAndSparkJobId(outputOpID: OutputOpId, sparkJobId: SparkJobId)

/**
	*  BatchUIData --> 对BatchInfo的封装 类似VO
	*  OutputOperationUIData  --> 对OutputOperationInfo的封装
	*/

case class BatchUIData(
												val batchTime: Time,
												val streamIdToInputInfo: Map[Int, StreamInputInfo],
												val submissionTime: Long,
												val processingStartTime: Option[Long],
												val processingEndTime: Option[Long],
												val outputOperations: mutable.HashMap[OutputOpId, OutputOperationUIData] =  mutable.HashMap(),
												var outputOpIdSparkJobIdPairs: Seq[OutputOperationIdAndSparkJobId] = Seq.empty
	) {

	/**
		* 当前Batch提交给调度器开始到第一个作业被开始处理的时间
		*/
	def schedulingDelay:Option[Long] = {
		for(pst <- processingStartTime)
			yield pst - submissionTime
	}

	/**
		* 当前Batch所有作业从开始到执行结束的耗时
		*/
	def processingDelay: Option[Long] = {
		for(pst <- processingStartTime; pet <- processingEndTime)
			yield pet - pst
	}

	/**
		* 从batch被提交到所有作业执行结束的时间
		*/
	def totalDelay: Option[Long] = {
		processingEndTime.map(_ - submissionTime)
	}

	/**
		*	当前Batch处理的所有数据条数
		*/
	def numTotalRecords: Long = {
		streamIdToInputInfo.values.map(_.numRecords).sum
	}


	def updateOutputOperationInfo(outputOperationInfo: OutputOperationInfo): Unit = {
		assert(batchTime == outputOperationInfo.batchTime)
		outputOperations(outputOperationInfo.id) = OutputOperationUIData(outputOperationInfo)
	}

	/**
		* OutputOperation中失败的个数
		*/
	def numFailedOutputOp: Int = {
		outputOperations.values.count(_.failureReason.nonEmpty)
	}

	/**
		* OutputOperation仍在运行的个数
		*/
	def numActiveOutputOp: Int = {
		outputOperations.values.count(_.endTime.isEmpty)
	}

	/**
		* 已完成的OutputOperation个数
		*/
	def numCompletedOutputOp: Int = {
		outputOperations.values.count{op =>
			op.failureReason.isEmpty && op.endTime.isEmpty
		}
	}

	def isFailed: Boolean = numFailedOutputOp != 0
}

object BatchUIData {
	def apply(batchInfo: BatchInfo): BatchUIData = {
		val outputOperations = mutable.HashMap[OutputOpId, OutputOperationUIData]()
		outputOperations ++= batchInfo.outputOperationInfos.mapValues(OutputOperationUIData.apply)

		new BatchUIData(
			batchInfo.batchTime,
			batchInfo.streamIdtoInputInfo,
			batchInfo.submissionTime,
			batchInfo.processingStartTime,
			batchInfo.processingEndTime,
			outputOperations
		)
	}
}


case class OutputOperationUIData(
		id: OutputOpId,
		name: String,
		description: String,
		startTime: Option[Long],
		endTime: Option[Long],
		failureReason: Option[String]
	) {

	def duration: Option[Long] = for (s <- startTime; e<- endTime) yield e - s
}

object OutputOperationUIData {
	def apply(outputOperationInfo: OutputOperationInfo): OutputOperationUIData = {
		new OutputOperationUIData(
			outputOperationInfo.id,
			outputOperationInfo.name,
			outputOperationInfo.description,
			outputOperationInfo.startTime,
			outputOperationInfo.endTime,
			outputOperationInfo.failureReason
		)
	}
}
