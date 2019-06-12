package org.apache.spark.streaming.receiver.handler

import org.apache.spark.storage._
import org.apache.spark.streaming.receiver.input.{ArrayBufferBlock, ByteBufferBlock, IteratorBlock, ReceivedBlock}
import org.apache.spark.streaming.receiver.output.{BlockManagerBasedStoreResult, ReceivedBlockStoreResult}

/**
	* 采用BlockManager存储数据
	*/
class BlockManagerBasedBlockHandler(
			 blockManager:BlockManager,
			 storageLevel: StorageLevel) extends ReceivedBlockHandler {

	override def storeBlock(blockId: StreamBlockId, receivedBlock: ReceivedBlock): ReceivedBlockStoreResult = {
		var numRecords: Option[Long] = None

		val putResult: Seq[(BlockId, BlockStatus)] = receivedBlock match {
			case ArrayBufferBlock(arrayBuffer) =>
				numRecords = Some(arrayBuffer.size.toLong)
				blockManager.putIterator(blockId, arrayBuffer.iterator, storageLevel,tellMaster = true)

			case IteratorBlock(iter) =>
				val countIterator  = new CountingIterator(iter)

				val putResult = blockManager.putIterator(blockId,countIterator,storageLevel, tellMaster = true)
				numRecords = countIterator.count
				putResult
			case ByteBufferBlock(byteBuf) =>
				blockManager.putBytes(blockId, byteBuf, storageLevel, tellMaster = true)
			case o =>
				throw new  Exception(s"无法存储blockid=${blockId}到BlockManager，不支持的block类型：${o.getClass.getName}")

		}

		if (!putResult.map(_._1).contains(blockId)) {
			throw new Exception(s"无法以存储级别${storageLevel}将${blockId}存储到BlockMan中")
		}

		BlockManagerBasedStoreResult(blockId,numRecords)
	}

	override def cleanupOldBlocks(threshTime: Long): Unit = {
		//采用BlockManager保存的Block数据，会通过DStream的clearMetadata
		// 方法在请求generatedRDDs是给删除
	}
}

/**
	* 具有统计元素个数功能的迭代器
	* @param iter
	* @tparam T
	*/
class CountingIterator[T](iter: Iterator[T]) extends Iterator[T] {
	private var _count =  0L

	override def hasNext: Boolean = iter.hasNext

	override def next(): T = {
		_count += 1
		iter.next()
	}

	private def isFullyConsumed: Boolean = !iter.hasNext

	def count(): Option[Long] = {
		if(isFullyConsumed) Some(_count) else None
	}
}