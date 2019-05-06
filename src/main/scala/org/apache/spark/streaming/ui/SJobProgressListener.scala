package org.apache.spark.streaming.ui

import org.apache.spark.Logging
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.streaming.{SContext, Time}
import org.apache.spark.streaming.domain.{BatchInfo, OutputOperationInfo, StreamInputInfo}
import org.apache.spark.streaming.scheduler.SListener

class SJobProgressListener(ssc: SContext) extends SListener with SparkListener with Logging{
	var runningBatches: Seq[BatchInfo] = Seq[BatchInfo](
		BatchInfo(Time(System.currentTimeMillis()), System.currentTimeMillis()-10000,Some(System.currentTimeMillis()- 9000),Some(System.currentTimeMillis() - 5000),Map(1->StreamInputInfo(1, 1000)), Map(1->OutputOperationInfo(Time(System.currentTimeMillis()-102380), 2,"op1","正在运行",Some(System.currentTimeMillis()-10230),Some(System.currentTimeMillis()-10030),None)))
	)

	var waitingBatches: Seq[BatchInfo] = Seq[BatchInfo](
		BatchInfo(Time(System.currentTimeMillis()), System.currentTimeMillis()-1000,Some(System.currentTimeMillis()- 800),Some(System.currentTimeMillis() - 5000),Map(1->StreamInputInfo(1, 1000)), Map(1->OutputOperationInfo(Time(System.currentTimeMillis()-10000), 2,"op1","等待运行",Some(System.currentTimeMillis()-10000),Some(System.currentTimeMillis()-10030),None)))
	)

	var retainedCompletedBatches: Seq[BatchInfo] = Seq[BatchInfo](
		BatchInfo(Time(System.currentTimeMillis()), System.currentTimeMillis()-30000,Some(System.currentTimeMillis()- 10000),Some(System.currentTimeMillis() - 5000),Map(1->StreamInputInfo(1, 1000)), Map(1->OutputOperationInfo(Time(System.currentTimeMillis()-38000), 2,"op1","已完成",Some(System.currentTimeMillis()-32000),Some(System.currentTimeMillis()-35000),None)))
	)

	logInfo("创建SJobProgressListener....")


	val batchDuration: Long = System.currentTimeMillis()
	var numTotalReceivedRecords = 999
	var numTotalCompletedBatches = 1000



}


object SJobProgressListener {
	type SparkJobId = Int
	type OutputOpId = Int
}