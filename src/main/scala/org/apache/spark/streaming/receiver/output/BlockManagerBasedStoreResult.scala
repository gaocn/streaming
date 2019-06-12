package org.apache.spark.streaming.receiver.output

import org.apache.spark.storage.StreamBlockId

/**
	* 使用ReceivedBlockHandler存储数据后的block的元数据信息
	*
	* @param blockId <>
	* @param numRecords
	*/
case class BlockManagerBasedStoreResult(blockId: StreamBlockId, numRecords: Option[Long]) extends ReceivedBlockStoreResult
