package org.apache.spark.streaming.scheduler

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.Logging
import org.apache.spark.util.AsynchronousListenerBus

class SListenerBus extends AsynchronousListenerBus[SListener, SListenerEvent]("SListenerBus") with Logging {
	logInfo("创建SListenerBus")

	private val logDroppedEvent = new AtomicBoolean(false)

	override def onDropEvent(event: SListenerEvent): Unit = {
		logInfo(s"SListenerBus onDropEvent: ${event}")
		if (logDroppedEvent.compareAndSet(false, true)) {
			//仅仅记录日志因此，以防止相同日志频繁打印
			logError("丢弃SListenerEvent，因为事件队列已满，可能" +
				"原因是SListener处理速度太慢不能跟上scheduler产生事件的速度！")
		}
	}

	override def onPostEvent(listener: SListener, event: SListenerEvent): Unit = {
		logInfo(s"${listener}向SListenerBus提交事件${event}")
		event match {
			case batchStarted: SListenerBatchStarted =>
				listener.onBatchStarted(batchStarted)
			case batchSubmitted: SListenerBatchSubmitted =>
				listener.onBatchSubmitted(batchSubmitted)
			case batchCompleted: SListenerBatchCompleted =>
				listener.onBatchCompleted(batchCompleted)
			case outputOperationStarted: SListenerOutputOperationStarted =>
				listener.onOutputOperationStarted(outputOperationStarted)
			case outputOperationCompleted: SLitenerOutputOperationCompleted =>
				listener.onOutputOperationCompleted(outputOperationCompleted)
			case receiverStarted: SListenerReceiverStarted =>
				listener.onReceiverStarted(receiverStarted)
			case receiverStopped: SListenerReceiverStopped =>
				listener.onReceiverStopped(receiverStopped)
			case receiverError: SListenerReceiverError =>
				listener.onReceiverError(receiverError)
			case _ =>
				logInfo(s"不发识别的事件")
		}
	}
}
