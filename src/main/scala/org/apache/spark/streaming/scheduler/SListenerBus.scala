package org.apache.spark.streaming.scheduler

import org.apache.spark.Logging
import org.apache.spark.util.AsynchronousListenerBus

class SListenerBus extends AsynchronousListenerBus[SListener, SListenerEvent]("SListenerBus") with Logging{
	logInfo("创建SListenerBus")

	override def onDropEvent(event: SListenerEvent): Unit = {
		logInfo(s"SListenerBus onDropEvent: ${event}")
	}

	override def onPostEvent(listener: SListener, event: SListenerEvent): Unit = {
		logInfo(s"${listener}向SListenerBus提交事件${event}")

	}
}
