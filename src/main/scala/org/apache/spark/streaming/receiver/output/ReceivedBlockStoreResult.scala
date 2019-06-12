package org.apache.spark.streaming.receiver.output

import org.apache.spark.storage.StreamBlockId

/**
	* 已存储block的元数据
	*/
trait ReceivedBlockStoreResult {
	/** 当前block存储后对应的blockid */
	def blockId: StreamBlockId

	/** 当前block中的记录条数 */
	def numRecords: Option[Long]
}
