package org.apache.spark.streaming.wal

import java.nio.ByteBuffer

/**
	* WAL(Write Ahead Log)抽象类，Streaming中用于保存接收器接收的数
	* 据同时将元数据存储在可靠的存储介质中，以便在Driver故障恢复。
	*
	* WAL顺序写，支持(基于时间索引的)随机读，因此WAL的写效率是很高的！
	*/
abstract class WriteAheadLog {

	/**
		* 顺序写入记录到日志文件中并返回一个日志句柄，通过日志句柄包含读取
		* 写入记录的元数据信息，时间用于索引写入记录，也用于基于时间清理过
		* 期的记录。
		*
		* PS：该方法需要确保在方法返回后，记录已被持久化并且能够通过WAL日志
		* 句柄对读取持久化记录。
		*
		* @param record 记录
		* @param time 索引写入记录
		* @return
		*/
	def write(record:ByteBuffer, time: Long): WriteAheadLogRecordHandle


	/**
		* 通过WAL句柄可以读取一次持久化后的记录，由于句柄可以根据时间索引记录，
		* 而不是通过全局扫描的方式，因此读取还是
		* @param handle
		* @return
		*/
	def read(handle: WriteAheadLogRecordHandle):ByteBuffer


	/**
		* WAL日志句柄所有写入且为被清理的日志记录的迭代器
		* @return
		*/
	def readAll(): Iterator[ByteBuffer]

	/**
		* 清理所有比指定时间早的数据，可以阻塞至清理完成后返回。
		* @param threshTime
		* @param waitForCompletion
		*/
	def clean(threshTime: Long, waitForCompletion: Boolean)


	/**
		* 关闭当前WAL日志句柄并释放所有资源
		*/
	def close()
}
