package org.apache.spark.streaming.scheduler

import org.apache.spark.Logging
import org.apache.spark.streaming.Time

private[streaming] class JobGenerator(jobScheduler: JobScheduler) extends Logging{
	def onCheckpointCompletion(checkpointTime: Time, clearCheckpointDataLater: Boolean) = ???

}
