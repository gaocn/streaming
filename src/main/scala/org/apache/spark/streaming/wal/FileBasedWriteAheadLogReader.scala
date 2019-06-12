package org.apache.spark.streaming.wal

import java.io.{Closeable, EOFException, IOException}
import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.spark.Logging
import org.apache.spark.util.HdfsUtils

/**
	* 读取通过[[FileBasedWriteAheadLogWriter]]写入日志文件的数据，将
	* 记录按照日志文件的顺序以ByteBuffer形式读取，并返回一个ByteBuffer
	* 的迭代器。
	*/
class FileBasedWriteAheadLogReader(path:String, hadoopConfig:Configuration) extends Iterator[ByteBuffer] with Closeable with Logging{
	private val instream = HdfsUtils.getInputStream(path, hadoopConfig)
	//当打开文件时，文件可能会被删除，此时返回空
	private var closed =  (instream == null)
	private var nextItem: Option[ByteBuffer] = None

	override def hasNext: Boolean = {
		if (closed) return false

		if(nextItem.isDefined) {
			return true
		} else {
			try {
				val length = instream.readInt()
				val buffer = new Array[Byte](length)
				instream.readFully(buffer)
				nextItem = Some(ByteBuffer.wrap(buffer))
				logInfo(s"从WAL日志文件中读取到数据：${nextItem.get}")
				true
			} catch {
				case e:EOFException =>
					logInfo(s"已经读到WAL文件[${path}]结尾：${e.getMessage}")
					close()
					false
				case e: IOException =>
					logWarning(s"读取WAL文件[${path}]内容时出错，若文件已被删除则没有问题。", e)
					close()
					if (HdfsUtils.checkFileExists(path,hadoopConfig))  {
						//若文件存在，说明出错，抛出异常
						throw e
					} else  {
						//文件被删除，可能是在恢复过程中后台清理线程过早运行
						false
					}
				case e: Exception =>
					logWarning(s"从HDFS读取WAL文件[${path}]内容时出错：${e.getMessage}")
					close()
					throw e
			}
		}
	}

	override def next(): ByteBuffer = synchronized{
		val data =  nextItem.getOrElse{
			close()
			throw new Exception("next called without calling hasNext or after hasNext returned false")
		}
		//确保下一次的hasNext调用会加载新的数据
		nextItem = null
		data
	}

	override def close(): Unit = {
		if(!closed) {
			instream.close()
		}
		closed = true
	}
}
