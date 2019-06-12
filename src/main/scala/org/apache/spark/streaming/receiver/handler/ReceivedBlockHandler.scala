package org.apache.spark.streaming.receiver.handler

import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.receiver.input.ReceivedBlock
import org.apache.spark.streaming.receiver.output.ReceivedBlockStoreResult

/** 存储data block的处理器 */
trait ReceivedBlockHandler {

	/** 存储指定block数据，并返回存储block的元数据 */
	def storeBlock(blockId: StreamBlockId, receivedBlock: ReceivedBlock):ReceivedBlockStoreResult

	/** 清理在指定时刻之前的所有blocks */
	def cleanupOldBlocks(threshTime: Long)
}
