package org.apache.spark.streaming.wal


/**
	* 代表WAL日志文件中的一个片段
	* @param path
	* @param offset
	* @param length
	*/
case class FileBasedWriteAheadLogSegment(path:String, offset:Long, length:Int) extends WriteAheadLogRecordHandle
