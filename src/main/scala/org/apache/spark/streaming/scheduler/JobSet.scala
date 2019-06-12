package org.apache.spark.streaming.scheduler

import org.apache.spark.Logging
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.domain.{BatchInfo, StreamInputInfo}

import scala.collection.mutable

/**
	* 某个批次中所包含的作业
	*/
case class JobSet(time: Time, jobs:Seq[Job],streamIdToInputInfo:Map[Int, StreamInputInfo]=Map.empty) extends Logging {
	private val incompleteJobs = new mutable.HashSet[Job]()
	//该批次作业提交时间
	private val submisstionTime = System.currentTimeMillis()
	//该批次第一个作业开始被处理时间
	private var processingStartTime =  -1L
	//该批次最后一个作业处理完成时间
	private var processingEndTime  = -1L

	jobs.zipWithIndex.foreach{case (job, i) => job.setOutputOpId(i)}
	incompleteJobs ++= jobs

	def handleJobStart(job:Job): Unit = {
		if(processingStartTime <  0) {
			processingStartTime = System.currentTimeMillis()
		}
	}

	def handleJobCompletion(job:Job): Unit = {
		incompleteJobs -= job
		if(hasCompleted) {
			processingEndTime = System.currentTimeMillis()
		}
	}

	def hasStarted:Boolean  = processingStartTime  > 0
	def hasCompleted:Boolean = incompleteJobs.isEmpty

	//仅仅包含处理时间，不包含排队时间
	def processingDelay:Long = processingEndTime - processingStartTime

	//包含作业调度(排队)时间和处理时间
	def totalDelay:Long = {
		processingEndTime - time.milliseconds
	}

	def toBatchInfo:BatchInfo =  {
		BatchInfo(
			time,
			submisstionTime,
			if(processingStartTime > 0)Some(processingStartTime) else None,
			if(processingEndTime > 0)Some(processingEndTime) else None,
			streamIdToInputInfo,
			jobs.map{job => (job.outputOpId, job.toOutputOperationInfo)}.toMap
		)
	}
}
