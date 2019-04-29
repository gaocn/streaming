package org.apache.spark.streaming.scheduler

import org.apache.spark.Logging
import org.apache.spark.streaming.{SContext, Time}

class JobScheduler(scontext: SContext) extends Logging{
	def getPendingTimes():Seq[Time] = ???
}
