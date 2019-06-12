package org.apache.spark.streaming.receiver.handler

import org.apache.hadoop.conf.Configuration
import org.apache.spark.storage.{BlockManager, StorageLevel, StreamBlockId}
import org.apache.spark.streaming.receiver.input.ReceivedBlock
import org.apache.spark.streaming.receiver.output.ReceivedBlockStoreResult
import org.apache.spark.util.{GClock, GSystemClock}
import org.apache.spark.util.GSystemClock
import org.apache.spark.{Logging, SparkConf}

/**
	* 将接收器接收的数据先存放在WAL日志中，然后利用BlockManger存放在
	* Spark内存或磁盘中
	*/
class WriteAheadLogBasedBlockHandler(
				blockManager: BlockManager,
				streamId: Int,
				storageLevel: StorageLevel,
				conf: SparkConf,
				hadoopConf: Configuration,
				checkpointDir: String,
				clock: GClock = new GSystemClock
			) extends ReceivedBlockHandler with Logging{



	/** 存储指定block数据，并返回存储block的元数据 */
	override def storeBlock(blockId: StreamBlockId, receivedBlock: ReceivedBlock): ReceivedBlockStoreResult = ???

	/** 清理在指定时刻之前的所有blocks */
	override def cleanupOldBlocks(threshTime: Long): Unit = ???

}

object WriteAheadLogBasedBlockHandler {

}
