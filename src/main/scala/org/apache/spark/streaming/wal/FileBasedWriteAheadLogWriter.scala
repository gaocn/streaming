package org.apache.spark.streaming.wal

import java.io.Closeable
import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.spark.util.HdfsUtils

import scala.util.Try

/**
	* 用于将ByteBuffer中的记录写入WAL日志文件中
	*/
class FileBasedWriteAheadLogWriter(path:String, hadoopConfig:Configuration) extends Closeable{

	private lazy val stream = HdfsUtils.getOutputStream(path, hadoopConfig)

	private lazy val hadoopFlushMethod = {
		//利用反射获取刷新函数
		val cls = classOf[FSDataOutputStream]
		Try(cls.getMethod("hflush")).orElse(Try(cls.getMethod("sync"))).toOption
	}

	private var nextOffset = stream.getPos
	private var closed = false

	def write(data:ByteBuffer):FileBasedWriteAheadLogSegment = synchronized{
		HdfsUtils.checkState(!closed, "Stream已关闭，创建新的Writer写文件")

		data.rewind()
		val lengthToWrite = data.remaining()
		val segment = new FileBasedWriteAheadLogSegment(path, nextOffset, lengthToWrite)
		stream.write(lengthToWrite)
		if(data.hasArray) {
			stream.write(data.array())
		} else {
			//若字节数组是直接内存中的，需要先将其拷贝到内存数组中再写入，比
			// 一个一个写入效率要高
			while (data.hasRemaining) {
				val array = new Array[Byte](data.remaining())
				data.get(array)
				stream.write(array)
			}
		}
		flush()
		nextOffset = stream.getPos
		segment
	}

	private def  flush(): Unit ={
		hadoopFlushMethod.foreach(_.invoke(stream))
		//对于本地文件系统，需使用如下方式刷新
		stream.getWrappedStream.flush()
	}

	override def close(): Unit = {
		closed = true
		stream.close()
	}
}
