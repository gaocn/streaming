package org.apache.spark.streaming.wal

import java.nio.ByteBuffer
import java.util.concurrent.LinkedBlockingQueue

import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkConf}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._
import scala.util.control.NonFatal

/**
	* WriteAheadLog的一个封装，按批次写入数据。
	*
	* 当写入一批记录到WAL日志文件时，write(time)中的时间应该为最后一条
	* 记录的时间，这样保证WAL日志文件不会被错误清理。
	*
	* 例如：接收的记录的时间为：1，3,5,7，若用"log-1"作为WAL日志文件名，
	* 在清理time=3时进行日志清理，此时"log-1"文件会被清理，但其中会有
	* 比time=3还要新的数据！
	*
	* 为了充分利用batch，调用者可以采用多线程方式进行写操作，在并发写入
	* 时会阻塞直到上一个写操作完成。
	*
	*/
class BatchedWriteAheadLog(val wrappedLog: WriteAheadLog, conf:SparkConf) extends WriteAheadLog with Logging{
	import BatchedWriteAheadLog._
	private val walWriteQueue = new LinkedBlockingQueue[Record]()

	//标识writer线程是否是active的
	@volatile
	private var active:Boolean = true

	private val buffer = new ArrayBuffer[Record]()

	private val batchedWriterThread = startBatchWriterThread()

	/**
		* 将字节缓冲区写入日志文件
		*
		* 方法将字节缓存区添加到队列中，阻塞直到成功写入。
		*/
	override def write(record: ByteBuffer, time: Long): WriteAheadLogRecordHandle = {
		val promise = Promise[WriteAheadLogRecordHandle]()
		val putSuccessfully = synchronized{
			if(active) {
				walWriteQueue.offer(Record(record, time, promise))
				true
			} else {
				false
			}
		}
		if(putSuccessfully) {
			Await.result(promise.future, WriteAheadLogUtils.getBatchingTimeout(conf) milliseconds)
		} else {
			throw new Exception(s"BatchedWriteAheadLog在写请求${time}结束前被关闭！")
		}

	}

	/**
		* 通过WAL句柄可以读取一次持久化后的记录，由于句柄可以根据时间索引记录，
		* 而不是通过全局扫描的方式，因此读取还是
		*
		* @param handle
		* @return
		*/
	override def read(handle: WriteAheadLogRecordHandle): ByteBuffer = {
		throw new UnsupportedOperationException("BatchedWriteAheadLog不支持read()操作，" +
			"因为数据需要de-aggregate")
	}

	/**
		* WAL日志句柄所有写入且为被清理的日志记录的迭代器
		*
		* @return
		*/
	override def readAll(): Iterator[ByteBuffer] = {
		wrappedLog.readAll().flatMap(deaggregate)
	}

	/**
		* 清理所有比指定时间早的数据，可以阻塞至清理完成后返回。
		*
		* @param threshTime
		* @param waitForCompletion
		*/
	override def clean(threshTime: Long, waitForCompletion: Boolean): Unit = {
		wrappedLog.clean(threshTime, waitForCompletion)
	}

	/**
		* 关闭当前WAL日志句柄并释放所有资源
		*/
	override def close(): Unit = {
		logInfo(s"BatchedWriteAheadLog在${System.currentTimeMillis()}时刻关闭")
		synchronized{
			active = false
		}
		batchedWriterThread.interrupt()
		batchedWriterThread.join()
		while (!walWriteQueue.isEmpty) {
			val Record(_, time, promise) = walWriteQueue.take()
			promise.failure(new IllegalStateException(s"BatchedWriteAheadLog在写请求${time}结束前被关闭！"))
		}
		wrappedLog.close()
	}

	/**
		* 在单独的线程中创建log writer
		*/
	private def startBatchWriterThread():Thread = {
		val thread = new Thread("BatchedWriteAheadLog-Writer") {
			override def run(): Unit = {
				while (active) {
					try {
						flushRecords()
					} catch{
						case NonFatal(e)  =>
							logWarning(s"BatchedWriteAheadLog-Writer线程出错：${e.getMessage}")
					}
				}
				logInfo("BatchedWriteAheadLog-Writer线程退出")
			}
		}

		thread.setDaemon(true)
		thread.start()
		thread
	}

	/**
		* 将buffer中的所有records写入WAL日志中
		*/
	private def flushRecords(): Unit = {
		try {
			buffer.append(walWriteQueue.take())
			val numBatched = walWriteQueue.drainTo(buffer.asJava) + 1
			logInfo(s"从队列中获取${numBatched}批次个recrods")
		} catch {
			case _:InterruptedException =>
				logWarning("BatchedWriteAheadLog-Writer线程被中断")
		}

		try {
			var segment:WriteAheadLogRecordHandle = null
			if (buffer.length > 0) {
				//多线程在写入批次时是乱序的，因此需要排序
				val sortedByTime = buffer.sortBy(_.time)

				//把最新的时间作为写入WAL日志文件的时间戳
				val time = sortedByTime.last.time
				segment = wrappedLog.write(aggregate(sortedByTime), time)
			}
			buffer.foreach(_.promise.success(segment))
		} catch {
			case e:InterruptedException =>
				logWarning("BatchedWriteAheadLog Writer queue被中断")
				buffer.foreach(_.promise.failure(e))
			case NonFatal(e) =>
				logWarning(s"BatchedWriteAheadLog Writer写${buffer}失败：${e.getMessage}")
				buffer.foreach(_.promise.failure(e))
		} finally {
			buffer.clear()
		}
	}

	/** 获取队列大小，测试时使用 */
	private def getQueueLength:Int = walWriteQueue.size()
}


object BatchedWriteAheadLog {

	/**
		* 代表要写入WAL日志文件的若干条记录的wrapper
		* @param data 要写入的一批数据
		* @param time 写请求的时间
		* @param promise 当另一个线程正在写入时会阻塞当前写请求
		*/
	case class Record(data: ByteBuffer, time:Long, promise:Promise[WriteAheadLogRecordHandle])

	private def getByteArray(buffer:ByteBuffer):Array[Byte] = {
		val byteArray = new Array[Byte](buffer.remaining())
		buffer.get(byteArray)
		byteArray
	}

	/**
		* 将多条记录序列化后，再聚合到一个ByteBuffer中
		* @param records
		* @return
		*/
	def aggregate(records: Seq[Record]):ByteBuffer = {
		ByteBuffer.wrap(Utils.serialize[Array[Array[Byte]]](
			records.map{record => getByteArray(record.data)}.toArray
		))
	}


	/**
		* 将聚合的多条记录反序列化后，载转换为ByteBuffer数组。
		*
		* A stream may not have used batching initially, but
		* started using it after a restart. This method therefore
		* needs to be backwards compatible.
		*
		*/
	def deaggregate(buffer: ByteBuffer):Array[ByteBuffer] = {
		try {
			Utils.deserialize[Array[Array[Byte]]](getByteArray(buffer))
  			.map(ByteBuffer.wrap)
		} catch {
			case _:ClassCastException =>
				//users may restart a stream with batching enabled
				Array(buffer)
		}
	}

}