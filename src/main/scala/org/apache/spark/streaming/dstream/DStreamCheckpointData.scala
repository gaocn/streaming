package org.apache.spark.streaming.dstream

import java.io.{IOException, NotSerializableException, ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.Logging
import org.apache.spark.streaming.Time

import scala.collection.mutable.HashMap
import scala.reflect.ClassTag

private[streaming]
class DStreamCheckpointData[T: ClassTag](dstream: DStream[T]) extends Serializable with Logging{
	protected val data = new HashMap[Time, AnyRef]()

	/** BatchTime与该批次checkpoined RDD文件名的映射*/
	@transient
	private var timeToCheckpointFile = new HashMap[Time, String]()

	/** BatchTime与该批次的checkpoint数据中最旧的checkpoined RDD的时间*/
	@transient
	private var timeToOldestCheckpointFileTime = new HashMap[Time, Time]()

	//HDFS 文件系统
	@transient
	private var fileSystem: FileSystem = null

	protected[streaming] def currentCheckpointFiles = {
		data.asInstanceOf[HashMap[Time, String]]
	}

	/**
		* 更新DStream的checkpoin数据，在每次DStreamGraph的checkpoint
		* 开始(initiated)时被调用。默认实现是记录DStream中的每个RDD保存
		* 的checkpoint文件名
		*/
	def update(time:Time): Unit = {

		//从产生的RDDs中获取对应的checkpoint文件名
		val checkpointFiles = dstream.generatedRDD.filter(_._2.getCheckpointFile.isDefined)
  		.map(x => (x._1, x._2.getCheckpointFile.get))
		logInfo(s"当前DStream中所有RDD的checkpoint文件为：${}")

		//添加需要被序列化的checkpoint文件名
		if(!checkpointFiles.isEmpty) {
			currentCheckpointFiles.clear()
			currentCheckpointFiles ++= checkpointFiles

			//用于删除旧的checkpoint文件
			timeToCheckpointFile ++= currentCheckpointFiles

			//记录当前状态下最旧的checkpoint RDD的时间
			timeToOldestCheckpointFileTime(time) = currentCheckpointFiles.keys.min(Time.ordering)
		}
	}

	/**
		* 该方法在每次checkpoint写入HDFS文件后被调用，以删除旧的
		* checkpoint数据。
		*/
	def clearup(time:Time): Unit = {
		timeToOldestCheckpointFileTime.remove(time) match {
			case Some(lastCheckpointFileTime) =>
				//找到所有比`lastCheckpointFileTime`旧的checkpointed
			  // RDD文件将其删除，因为这些文件不会被使用，即使当master失
				// 效重启时，因为恢复时不会使用这些旧的checkpointed文件的数据
				val fileToDelete = timeToCheckpointFile.filter(_._1 < lastCheckpointFileTime)
				logInfo(s"需要删除的checkpoint文件为：${fileToDelete.mkString(",")}")

				fileToDelete.foreach{ case (time, file) =>
					try {
						val path = new Path(file)
						if (fileSystem == null) {
							fileSystem = path.getFileSystem(dstream.ssc.sparkContext.hadoopConfiguration)
						}
						fileSystem.delete(path, true)
						timeToCheckpointFile -= time
						logInfo(s"成功删除${time}时刻的checkpoint文件: ${file}")
					} catch {
						case e:Exception =>
							logWarning("未成功删除${time}时刻的checkpoint文件: ${file}")
							fileSystem = null
					}
				}

			case None =>
				logInfo("不需要请求checkpoint文件")
		}

	}

	/**
		* 恢复checkpoint数据，每次DStreamGraph从checkpoint文件恢复后
		* 被调用。默认实现是从checkpoint文件中恢复RDD
		*/
	def restore(): Unit = {
		//从checkpoint文件中创建RDD
		currentCheckpointFiles.foreach{case (time, file)  =>
			logInfo("成功恢复${time}时刻的checkpoint文件: ${file}")
			dstream.generatedRDD += ((time, dstream.ssc.sparkContext.checkpointFile[T](file)))
		}
	}

	private def writeObject(oos: ObjectOutputStream): Unit = {
		try {
			logInfo(s"${this.getClass.getSimpleName}.writeObject方法被调用")
			if (dstream.ssc.graph != null) {
				dstream.ssc.graph.synchronized {
					if (dstream.ssc.graph.checkpointInProgress) {
						oos.defaultWriteObject()
					} else {
						val msg = s"需要将${this.getClass.getSimpleName}对象作为RDD" +
							s"操作闭包的一部分被序列化，因为DStream实例在闭包内被引用。因此需" +
							s"要覆写当前DStream实例的RDD操作以避免该异常。通过当前操作可以避免Spark" +
							s"任务中一些无用的对象占用有限的内存资源。"
						throw new NotSerializableException(msg)
					}
				}
			} else {
				throw new NotSerializableException("DStream序列化时，对应的DStreamGraph实例为空！")
			}
		} catch {
			case e:IOException =>
				logError(s"序列化异常：${e.getMessage}")
				throw e
			case e: Exception  =>
				logError(s"序列化异常：${e.getMessage}")
				throw new IOException(e.getMessage)
		}
	}

	private def readObject(ois: ObjectInputStream): Unit = {
		try {
			logInfo(s"${this.getClass.getSimpleName}.readObject方法被调用")

			ois.defaultReadObject()
			timeToCheckpointFile = new HashMap[Time, String]
			timeToOldestCheckpointFileTime = new HashMap[Time, Time]
		} catch {
			case e:IOException =>
				logError(s"反序列化异常：${e.getMessage}")
				throw e
			case e: Exception  =>
				logError(s"反序列化异常：${e.getMessage}")
				throw new IOException(e.getMessage)
		}
	}


	override def toString: String = {
		s"[\n${currentCheckpointFiles.size}个checkpoint文件\n " +
			s"${currentCheckpointFiles.mkString("\n")} \n]"
	}
}
