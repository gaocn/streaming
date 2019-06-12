package org.apache.spark.streaming.wal

import java.io.Closeable
import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.HdfsUtils

/**
	* 随机读取WAL日志文件的reader，根据给定的 File Segment信息从WAL
	* 日志文件中读取记录并返回ByteBuffer。
	*/
class FileBasedWriteAheadLogRandomReader(path:String, hadoopConfig:Configuration) extends Closeable{
	private val instream = HdfsUtils.getInputStream(path, hadoopConfig)
	//若打开文件时，文件已被删除则会返回空
	private var closed = (instream == null)

	def read(segment: FileBasedWriteAheadLogSegment):ByteBuffer = {
		assertOpen()
		instream.seek(segment.offset)
		val nextLength = instream.readInt()
		HdfsUtils.checkState(nextLength  == segment.length, s"期待从WAL日志文件读取的长度为${segment.length}，当文件实际可读取的长度为${nextLength}")
		val buffer = new Array[Byte](nextLength)
		instream.readFully(buffer)
		ByteBuffer.wrap(buffer)
	}

	override def close(): Unit = {
		closed = true
		instream.close()
	}

	private def assertOpen(): Unit = {
		HdfsUtils.checkState(!closed, "stream已经关闭，需要创建一个新的Reader！")
	}
}
