package org.apache.spark.streaming.dstream.input

import java.io.InputStream

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.SContext
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.receiver.impl.SocketReceiver

import scala.reflect.ClassTag


class SocketInputDStream[T:ClassTag](
	ssc_ : SContext,
	host:String,
	port:Int,
	byteToObjects:InputStream => Iterator[T],
	storageLevel:StorageLevel) extends ReceiverInputDStream[T](ssc_) {

	/**
		* 任何[[ReceiverInputDStream]]的实现类都需要实现该方法，用于获
		* 取Receiver实例，然后发送到Worker节点上运行实时接收数据。
		*
		* @return
		*/
	override def getReceiver(): Receiver[T] = {
		new SocketReceiver(host, port, byteToObjects, storageLevel)
	}
}