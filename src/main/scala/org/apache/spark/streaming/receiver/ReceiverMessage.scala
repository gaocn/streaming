package org.apache.spark.streaming.receiver

import org.apache.spark.streaming.Time

/**
	* 发送到接收器Receiver的消息
	*/
private[streaming] sealed trait ReceiverMessage extends Serializable

private[streaming] object StopReceiver extends ReceiverMessage
private[streaming] case class CleanupOldBlocks(threshTime:Time) extends ReceiverMessage
private[streaming] case class UpdateRateLimit(elementsPerSecond: Long) extends ReceiverMessage