package org.apache.spark.streaming.scheduler

import org.apache.spark.Logging
import org.apache.spark.streaming.{SContext, Time}

class JobScheduler(scontext: SContext) extends Logging{
	logInfo("创建JobScheduler")

	val listenerBus = new SListenerBus()

	def getPendingTimes():Seq[Time] = ???

	def start(): Unit ={
		logInfo("启动JobScheduler调度器....")
	}

	def stop(processAllReceivedData:Boolean): Unit = {
		logInfo("停止JobScheduler调度器....")
	}
}

private[streaming] object JobScheduler {
	val BATCH_TIME_PROPERTY_KEY = "spark.streaming.internal.batchTime"
	val OUTPUT_OP_ID_PROPERTY_KEY = "spark.streaming.internal.outputOpId"
}