package org.apache.spark.streaming.wal

import java.nio.ByteBuffer
import java.util.concurrent.{RejectedExecutionException, ThreadPoolExecutor}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.util.HdfsUtils
import org.apache.spark.util.{CompletionIterator, ThreadUtils}
import org.apache.spark.{Logging, SparkConf}

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.{Await, ExecutionContext, Future}

/**
	* 用于管理WAL文件
	* 1、将ByteBuffer中的记录写入周期滚动的日志文件中；
	* 2、错误恢复，从日志文件中恢复；
	* 3、清理旧的日志文件；
	*
	* 使用[[FileBasedWriteAheadLogWriter]]写
	* 使用[[FileBasedWriteAheadLogReader]]读
	*
	* @param logDirectory 滚动日志文件所在的目录
	* @param hadoopConfig 读写日志文件时需要的配置信息
	*/
class FileBasedWriteAheadLog(
			conf:SparkConf,
			logDirectory: String,
			hadoopConfig: Configuration, //代表支持的文件系统类型，如本地磁盘或HDFS
			rollingIntervalSecs: Int,
			maxFailure: Int,
			closeFileAfterWrite: Boolean
		) extends WriteAheadLog with Logging{
	import FileBasedWriteAheadLog._
	private val pastLogs = new ArrayBuffer[LogInfo]
	private val callerNameTag = getCallerName.map(c=>s"for $c").getOrElse("")

	private val threadPoolName = s"WriteAheadLogManager ${callerNameTag}"
	private val threadPool = ThreadUtils.newDaemonCachedThreadPool(threadPoolName, 20)
	private val executionContext = ExecutionContext.fromExecutorService(threadPool)

	private var currentLogPath:Option[String] = None
	private var currentLogWriter: FileBasedWriteAheadLogWriter =  null
	private var currentLogWriterStartTime: Long = -1L
	private var currentLoWriterStopTime: Long = -1L

	initializeOrRecover()

	/**
		* 顺序写入记录到日志文件中并返回一个日志句柄，通过日志句柄包含读取
		* 写入记录的元数据信息，时间用于索引写入记录，也用于基于时间清理过
		* 期的记录。
		*
		* PS：该方法需要确保在方法返回后，记录已被持久化并且能够通过WAL日志
		* 句柄对读取持久化记录。
		*
		* @param record 记录
		* @param time   索引写入记录
		* @return
		*/
	override def write(record: ByteBuffer, time: Long): FileBasedWriteAheadLogSegment = synchronized{
		var fileSegment: FileBasedWriteAheadLogSegment = null
		var failures =  0
		var lastException: Exception = null
		var succeeded = false

		while(!succeeded && failures < maxFailure) {
			try {
				fileSegment = getLogWriter(time).write(record)
				if (closeFileAfterWrite) {
					resetWriter()
				}
				succeeded = true
			} catch {
				case e: Exception =>
					lastException = e
					logWarning(s"WAL日志文件写入出错：${e.getMessage}")
					resetWriter()
					failures += 1
			}
		}

		if(fileSegment == null) {
			logError(s"写入WAL日志文件失败，失败次数：${failures}")
			throw lastException
		}

		fileSegment
	}

	/**
		* 通过WAL句柄可以读取一次持久化后的记录，由于句柄可以根据时间索引记录，
		* 而不是通过全局扫描的方式，因此读取还是
		*
		* @param handle
		* @return
		*/
	override def read(handle: WriteAheadLogRecordHandle): ByteBuffer = {
		val fileSegment = handle.asInstanceOf[FileBasedWriteAheadLogSegment]
		var reader: FileBasedWriteAheadLogRandomReader = null
		var byteBuffer:ByteBuffer = null

		try {
			reader = new FileBasedWriteAheadLogRandomReader(fileSegment.path, hadoopConfig)
			byteBuffer = reader.read(fileSegment)
		} finally {
			reader.close()
		}
		byteBuffer
	}

	/**
		* WAL日志句柄所有写入且为被清理的日志记录的迭代器
		*
		* 一般是在调用者初始化并且希望从WAL日志恢复状态时(在开始写操作之前)
		* 调用，若调用者已经进行了写操作然后再调用该方法尝试恢复状态，该方法
		* 可能不会恢复最新的状态，因为默认不会处理当前活跃的WAL日志文件，
		* 因此相关实现代码就直接跳过
		*/
	override def readAll(): Iterator[ByteBuffer] = {
		val logFilesToRead = pastLogs.map(_.path) ++ currentLogPath
		logInfo(s"尝试从WAL日志文件中读取数据：${logFilesToRead.mkString("\n")}")

		def readFile(file:String):Iterator[ByteBuffer] = {
			logInfo(s"创建读取${file}文件的reader")
			val reader = new FileBasedWriteAheadLogReader(file, hadoopConfig)
			CompletionIterator[ByteBuffer, Iterator[ByteBuffer]](reader, reader.close())
		}
		if (!closeFileAfterWrite) {
			logFilesToRead.iterator.map(readFile).flatten
		} else {
			//若closeFileAfterWrite = true，为了提高性能可以并行进行文件恢复
			seqToParIterator(threadPool, logFilesToRead, readFile)
		}
	}

	/**
		* 清理所有比指定时间早的数据，可以阻塞至清理完成后返回。
		*
		* threshold time是基于日志文件的时间戳进行判断，时间戳通常是local
		* system的时间。因此若Driver节点指定的threshold time对不同的work
		* 节点来说可能会存在`时钟漂移`，需要调用者解决时钟漂移的问题！
		*
		* 若waitForCompletion=true，则该方法只有在所有旧的日志文件被删
		* 除完成后才会返回。一般在测试时将其设置为true，若设置为false则会
		* 异步进行删除
		*
		*/
	override def clean(threshTime: Long, waitForCompletion: Boolean): Unit = {
		val oldLogFiles = synchronized{
			val expiredLogs = pastLogs.filter(_.endTime < threshTime)
			pastLogs --= expiredLogs
			expiredLogs
		}

		logInfo(s"尝试删除在${logDirectory}下的${oldLogFiles.size}个在${threshTime}之前的旧的日志文件：${oldLogFiles.mkString("\n")}")

		def deleteFile(walInfo: LogInfo): Unit = {
			try {
				val path = new Path(walInfo.path)
				val fs = HdfsUtils.getFileSystemForPath(path, hadoopConfig)
				fs.delete(path, true)
				logInfo(s"删除文件：${walInfo}")
			} catch {
				case ex:Exception =>
					logWarning(s"删除文件${walInfo}时出错：${ex.getMessage}")
			}
		}

		oldLogFiles.foreach{logInfo =>
			if (!executionContext.isShutdown) {
				try{
					val f = Future{deleteFile(logInfo)}(executionContext)
					if (waitForCompletion) {
						import scala.concurrent.duration._
						Await.ready(f, 1 second)
					}
				}catch{
					case e: RejectedExecutionException =>
						logWarning(s"在删除旧的WAL日志是ExecutionContext被关闭，可能会影响恢复的准确性：${e.getMessage}")
				}
			}
		}
	}

	/**
		* 关闭当前WAL日志句柄并释放所有资源
		*/
	override def close(): Unit = {
		if(currentLogWriter != null) {
			currentLogWriter.close()
		}
		executionContext.shutdown()
		logInfo("停止Write Ahead Log机制")
	}


	private def getLogWriter(currentTime:Long):FileBasedWriteAheadLogWriter = synchronized{
		if (currentLogWriter == null || currentTime > currentLoWriterStopTime) {
			resetWriter()
			currentLogPath.foreach{
				pastLogs += LogInfo(currentLogWriterStartTime, currentLoWriterStopTime,  _)
			}
			currentLogWriterStartTime = currentTime
			currentLoWriterStopTime = currentTime + (rollingIntervalSecs * 1000)
			val newPath = new Path(logDirectory, timeToLogFile(currentLogWriterStartTime, currentLoWriterStopTime))

			currentLogPath = Some(newPath.toString)

			currentLogWriter = new FileBasedWriteAheadLogWriter(currentLogPath.get, hadoopConfig)
		}
		currentLogWriter
	}

	/**
		* 初始化日志文件所在目录，或从目录中恢复
		*/
	private def initializeOrRecover():Unit = synchronized{
		val logDirectoryPath = new Path(logDirectory)
		val fileSystem = HdfsUtils.getFileSystemForPath(logDirectoryPath, hadoopConfig)
		if(fileSystem.exists(logDirectoryPath) && fileSystem.getFileStatus(logDirectoryPath).isDirectory) {
			val logFileInfo = logFilesToLogInfo(fileSystem.listStatus(logDirectoryPath).map(_.getPath))

			pastLogs.clear()
			pastLogs ++= logFileInfo
			logInfo(s"从WAL目录恢复${logFileInfo.size}个日志文件")
			logInfo(s"已恢复的文件为：${logFileInfo.map(_.path).mkString("\n")}")
		}
	}

	private def resetWriter() =  synchronized{
		if(currentLogWriter != null) {
			currentLogWriter.close()
			currentLogWriter = null
		}
	}

}

object FileBasedWriteAheadLog {
	case class LogInfo(startTime: Long, endTime:Long, path:String)

	val logFileRegex = """log-(\d+)-(\d+)""".r

	def timeToLogFile(startTime:Long, stopTime:Long):String = {
		s"log-${startTime}-${stopTime}"
	}

	def getCallerName():Option[String] = {
		val stackTraceClasses = Thread.currentThread().getStackTrace().map(_.getClassName)
		stackTraceClasses.find(!_.contains("WriteAheadLog")).flatMap(_.split("\\.").lastOption)
	}

	/**
		* 将一系列文件应该为一些列LogInfo对象，并按照创建时间递增排序
		*/
	def logFilesToLogInfo(files:Seq[Path]):Seq[LogInfo] = {
		files.flatMap{file =>
			logFileRegex.findFirstIn(file.getName()) match {
				case Some(logFileRegex(startTimeStr, stopTimeStr)) =>
					Some(LogInfo(startTimeStr.toLong, stopTimeStr.toLong, file.toString))
				case None => None
			}
		}.sortBy(_.startTime)
	}

	/**
		* 在并行进行日志恢复时，我们希望最多创建不超过K个FileBasedWriteAheadLogReader实例，
		* 此时可以用该方法创建可并行工作的多个迭代器，并且保存任何时候内存
		* 中最多有`N`个实例，其中`N`为线程池大小。
		*/
	def seqToParIterator[I, O](
		tpool: ThreadPoolExecutor,
		source: Seq[I],
		handler: I=>Iterator[O]):Iterator[O] = {

		val taskSupport = new ForkJoinTaskSupport()
		val groupSize = tpool.getMaximumPoolSize.max(8)
		source.grouped(groupSize).flatMap{group =>
			val parallelCollection = group.par
			parallelCollection.tasksupport = taskSupport
			parallelCollection.map(handler)
		}.flatten
	}
}