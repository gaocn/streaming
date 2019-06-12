package org.apache.spark.streaming.receiver

/**
	* ReceiverTrackerEndpoint与driver本地通信的消息
	*/
private[streaming] sealed trait ReceiverTrackerLocalMessage

/** ReceiverTrackerEndpoint接收该消息会启动Spark Job运行receivers */
private[streaming] case class RestartReceiver(receiver: Receiver[_]) extends ReceiverTrackerLocalMessage

private[streaming] case class StartAllReceivers(receivers: Seq[Receiver[_]]) extends ReceiverTrackerLocalMessage

/** ReceiverTrackerEndpoint接收该消息会给所有注册的receivers发送停止信号，停止接收器 */
private[streaming] case object StopAllReceivers extends ReceiverTrackerLocalMessage

/** ReceiverTracker发送该消息请求在ReceiverTrackerEndpoint已注册的所有接收器的ids */
private[streaming] case object  AllReceiverIds extends ReceiverTrackerLocalMessage


/** 更新某个接收器的接收速率 */
private[streaming] case class UpdateReceiverRateLimit(streamId: Int, newRate: Long) extends ReceiverTrackerLocalMessage