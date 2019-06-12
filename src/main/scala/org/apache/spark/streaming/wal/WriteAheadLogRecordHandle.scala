package org.apache.spark.streaming.wal

/**
	* WAL中的句柄，负责处理记录的读写，其中需要包含读写所需的元数据，由
	* WriteAheadLog返回。
	*
	* @see WriteAheadLog
	*/
abstract class WriteAheadLogRecordHandle extends Serializable
