package org.apache.spark.streaming.receiver

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.streaming.domain.ReceivedBlockInfo


/**
	* 用于实现Receiver与ReceiverTracker的通信的消息
	*/
private[streaming] sealed trait ReceiverTrackerMessage

private[streaming] case class RegisterReceiver(streamId:Int, typ: String, host: String, executorId: String, receiverEndpoint: RpcEndpointRef)  extends ReceiverTrackerMessage

private[streaming] case class AddBlock(receivedBlockInfo: ReceivedBlockInfo) extends ReceiverTrackerMessage

private[streaming] case class ReportError(streamId:Int, message: String, error:String) extends ReceiverTrackerMessage

private[streaming] case class DeregisterReceiver(streamId:Int, msg:String, error:String) extends ReceiverTrackerMessage
