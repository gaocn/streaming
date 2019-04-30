package org.apache.spark.streaming.scheduler

import org.apache.spark.Logging
import org.apache.spark.streaming.Time

private[streaming] class JobGenerator(jobScheduler: JobScheduler) extends Logging{
	logInfo("创建JobGenerator")
	def onCheckpointCompletion(checkpointTime: Time, clearCheckpointDataLater: Boolean) = ???

}
