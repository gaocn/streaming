package org.apache.spark.streaming.receiver

import org.apache.spark.storage.StreamBlockId

import scala.collection.mutable.ArrayBuffer

/**
	* 处理Block Generator事件
	*/
trait BlockGeneratorListener {

	/**
		* 当data item被添加到BlockGenerator`后`调用。添加数据和调用该回
		* 调函数在block generation操作内部使用同步(synchronized)进行的，
		* 因此block generation会阻塞至数据添加和回调方法完成，这样做是确
		* 保当数据添加成功时对应的元数据也要更新成功，这对于block产生后就
		* 要立即使用元数据的操作非常有用。
		*
		* NOTE：该回调方法内部若存在阻塞操作将影响应用程序的吐吞量。
		*
		* @param data
		* @param metadata 元数据
		*/
	def  onAddData(data: Any, metadata: Any):Unit


	/**
		* 当Block Generator产生一个新的Block`时`被调用，block generation
		* 和该回调函数与添加数据及添加数据的回调方法采用synchronized同步
		* 完成，添加数据会阻塞等待block generation和其callback的完成。
		* 这样做是确保当数据添加成功时对应的元数据也要更新成功，这对于block
		* 产生后就要立即使用元数据的操作非常有用。
		*
		* NOTE：该回调方法内部若存在阻塞操作将影响应用程序的吐吞量。
		*
		* @param blockId
		*/
	def onGenerateBlock(blockId: StreamBlockId): Unit

	/**
		* 当某Block可以被push`时`调用，调用者应该在该方法内部将block存放到
		* Spark中，内部采用单线程调用该方法，没有与其他回调函数同步使用，因
		* 此内部可以进行阻塞时间长的操作。
		* @param blockId
		* @param arrayBuffer
		*/
	def onPushBlock(blockId: StreamBlockId, arrayBuffer: ArrayBuffer[_]):Unit


	/**
		* BlockGenerator内部发生错误`时`被调用，该方法会被多个地方调用，因
		* 此内部不建议使用任务阻塞时间长的操作。
		* @param message
		* @param cause
		*/
	def onError(message: String, cause: Throwable):Unit
}
