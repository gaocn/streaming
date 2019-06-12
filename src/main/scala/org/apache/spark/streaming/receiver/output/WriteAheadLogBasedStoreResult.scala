package org.apache.spark.streaming.receiver.output

import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.wal.WriteAheadLogRecordHandle

case class WriteAheadLogBasedStoreResult(
				blockId: StreamBlockId,
				numRecords: Option[Long],
				walRecordhandle: WriteAheadLogRecordHandle
				) extends ReceivedBlockStoreResult