package org.apache.spark.streaming.rdd

import java.io.File
import java.nio.ByteBuffer
import java.util.UUID

import org.apache.spark.{Partition, SparkContext, SparkEnv, TaskContext}
import org.apache.spark.rdd.BlockRDD
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.util.HdfsUtils
import org.apache.spark.streaming.wal.{FileBasedWriteAheadLogSegment, WriteAheadLog, WriteAheadLogRecordHandle, WriteAheadLogUtils}
import org.apache.spark.util.SerializableConfiguration

import scala.reflect.ClassTag
import scala.util.control.NonFatal


/**
	* WriteAheadLogBackedRDD的分区信息，每个分区与一个BlockId都对应，
	* 通过WriteAheadLogRecordHandle获取Block中的数据。
	*
	* @param index 分区编号
	* @param blockId 当前分区数据所在的BlockId
	* @param isBlockIdValid 当前BlockId是否有效，若无效说明数据已被
	*                       清理，应返回空。否则说明数据存放在executor中。
	* @param walRecordHandle 用于读写预写日志中数据的句柄
	*/
class WriteAheadLogBackedRDDPartition(
		 val index: Int,
		 val blockId:BlockId,
		 val isBlockIdValid: Boolean,
		 val walRecordHandle: WriteAheadLogRecordHandle) extends Partition

/**
	*
	* BlockRDD的一个特例，表示数据不仅存放在BlockManger中的，数据也会
	* 在WAL预写日志中保留一份。
	* 读数据步骤如下：
	*		1、根据BlockId从BlockManger中查找数据；
	* 	2、若没有找到，则使用WriteAheadLogRecordHandle从WAL中查找数据；
	*
	* 若isBlockIdValid设为false，可以跳过从BlockManger查询数据的过
	* 程，若知道BlockId已经不存在与executors中，例如Driver重启时，可
	* 以将isBlockIdValid=false优化读取速率。
	* @param sc
	* @param _blockIds 当前RDD中包含的数据
	* @param walRecordHandles 读写WAL日志中的RDD数据的句柄
	* @param isBlockIdValid 对应的BlockIds的数据是否还存在与
	*                            Executor中，若为false则跳过从BlockManger
	*                            查询数据的过程，直接从WAL中读取。
	* @param storeInBlockManager 当数据从WAL日志读取后是否将其存储
	*                            在BlockManger中。
	* @param storageLevel 数据存放在BlockManger中时的存储级别
	* @tparam T 数据元素类型
	*/
class WriteAheadLogBackedBlockRDD[T:ClassTag](
	 sc: SparkContext,
	 @transient private val _blockIds: Array[BlockId],
	 @transient val walRecordHandles: Array[WriteAheadLogRecordHandle],
	 @transient val isBlockIdValid: Array[Boolean] = Array.empty,
	 storeInBlockManager: Boolean = false,
	 storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER
 ) extends BlockRDD[T](sc,_blockIds){

	require(_blockIds.length == walRecordHandles.length, s"BlockIds数量[${_blockIds.length}]与WAL句柄数量[${walRecordHandles.length}]不一致")
	require(isBlockIdValid.isEmpty || isBlockIdValid.length == _blockIds.length, s"BlockIds数量[${_blockIds.length}]与isBlockIdValid数量[${isBlockIdValid.length}]不一致")

	/** Hadoop Config不可序列化，因此将其广播为一个序列化变量 */
	@transient private val hadoopConfig = sc.hadoopConfiguration
	private val  broadcastedHadoopConf = new SerializableConfiguration(hadoopConfig)

	override def isValid: Boolean = true

	override def getPartitions: Array[Partition] = {
		assertValid()
		Array.tabulate(_blockIds.length){ i=>
			val isValid = if(isBlockIdValid.length == 0) true else isBlockIdValid(i)
			new WriteAheadLogBackedRDDPartition(i, _blockIds(i), isValid, walRecordHandles(i))
		}
	}

	/**
		* 首先从BlockManger中读取分区的数据，若Executor内存中不存在则通
		* 过WAL日志读取该分区对应的数据。
		*/
	override def compute(split: Partition, context: TaskContext): Iterator[T] = {
		assertValid()
		val hadoopConf = broadcastedHadoopConf.value
		val blockManager = SparkEnv.get.blockManager
		val partition = split.asInstanceOf[WriteAheadLogBackedRDDPartition]
		val blockId = partition.blockId

		def getBlockFromBlockManger():Option[Iterator[T]] = {
			blockManager.get(blockId).map(_.data.asInstanceOf[Iterator[T]])
		}

		def getBlockFromWriteAheadLog():Iterator[T] = {
			var dataRead: ByteBuffer = null
			var writeAheadLog: WriteAheadLog = null

			try {
				/**
					* The WriteAheadLogUtils.createLog***创建WriteAheadLog
					* 对象需要指定目录，默认创建的FileBasedWriteAheadLog需要指
					* 定目录存放日志数据。如果只是读取那么就不需要目录，可以执行一
					* 个dummy路径以满足参数要求。
					*
					* 此时FileBasedWriteAheadLog不会在该dummy目录下创建文件
					* 或目录，并且该dummy不应该存在否则WAL会尝试从空目录恢复，并
					* 最终会报错！
					*/
				val nonExistentDirectory = new File(
					System.getProperty("java.io.tmpdir"),
					UUID.randomUUID().toString
				).getAbsolutePath

				writeAheadLog = WriteAheadLogUtils.createLogForReceiver(SparkEnv.get.conf, nonExistentDirectory, hadoopConf)
				dataRead = writeAheadLog.read(partition.walRecordHandle)
			} catch {
				case NonFatal(e) =>
					throw new Exception(s"无法从WAL日志文件中读取${partition.walRecordHandle}中的内容：${e.getMessage}")
			} finally {
				if(writeAheadLog != null) {
					writeAheadLog.close()
					writeAheadLog = null
				}
			}

			if (dataRead == null) {
				throw new Exception("无法从WAL日志文件中读取${partition.walRecordHandle}中的内容" +
					"，返回为空！")
			}

			logInfo(s"从${this}读取WAL日志数据，partition=${partition.index}")
			if(storeInBlockManager) {
				blockManager.putBytes(blockId, dataRead, storageLevel)
				logInfo(s"存储partition=${partition.index}到BlockManger中，存储级别为${storageLevel}")
				dataRead.rewind()
			}
			blockManager.dataDeserialize(blockId, dataRead).asInstanceOf[Iterator[T]]
		}


		if(partition.isBlockIdValid) {
			getBlockFromBlockManger().getOrElse{getBlockFromWriteAheadLog()}
		} else {
			getBlockFromWriteAheadLog()
		}
	}

	/**
		* 获取分区的优先存储位置
		*
		* 若数据存放在BlockManger中，则返回BlockManger所在的位置，否则
		* 若是WAL日志中读取，则对应FileSegment在HDFS中的位置。
		*/
	override def getPreferredLocations(split: Partition): Seq[String] = {
		val partition = split.asInstanceOf[WriteAheadLogBackedRDDPartition]
		val blockLocations = if(partition.isBlockIdValid) {
			getBlockIdLocations().get(partition.blockId)
		} else  {
			None
		}

		blockLocations.getOrElse{
			partition.walRecordHandle match {
				case fileSegment: FileBasedWriteAheadLogSegment =>
					try {
						HdfsUtils.getFileSegmentLocations(fileSegment.path,fileSegment.offset, fileSegment.length, hadoopConfig)
					} catch {
						case NonFatal(e) =>
							logError(s"获取file segment位置失败: ${e.getMessage}")
							Seq.empty
					}
				case _  =>
							Seq.empty
			}
		}
	}
}
