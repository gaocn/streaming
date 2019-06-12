package org.apache.spark.streaming.domain

import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.receiver.output.{ReceivedBlockStoreResult, WriteAheadLogBasedStoreResult}
import org.apache.spark.streaming.wal.WriteAheadLogRecordHandle

/**
	* 接收器接收到的blocks信息
	*/
private[streaming]case class ReceivedBlockInfo(
		streamId: Int,
		numRecords: Option[Long],
		metadataOption: Option[Any],
		blockStoreResult: ReceivedBlockStoreResult
	) {

	//fixme
	require(numRecords.isEmpty || numRecords.get >= 0, "记录数不能为负")

	@transient
	private var _isBlockIdValid = true

	def blockId: StreamBlockId = blockStoreResult.blockId

	def walRecordHandleOption: Option[WriteAheadLogRecordHandle] = {
		blockStoreResult match {
			case walStoreResult: WriteAheadLogBasedStoreResult=>
				Some(walStoreResult.walRecordhandle)
			case _ => None
		}
	}

	/**
		* 当前block是否存在与Executor中的内存或磁盘中
		* @return
		*/
	def isBlockIdValid():Boolean = _isBlockIdValid

	/**
		* Executor中已经不存在当前block时设置
		*/
	def setBlockIdInvalid():Unit  = {
		_isBlockIdValid = false
	}
}
