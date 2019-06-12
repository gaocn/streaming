package org.apache.spark.streaming.dstream.input

import org.apache.spark.rdd.{BlockRDD, RDD}
import org.apache.spark.storage.BlockId
import org.apache.spark.streaming.domain.{ReceivedBlockInfo, StreamInputInfo}
import org.apache.spark.streaming.{SContext, Time}
import org.apache.spark.streaming.rate.{RateController, RateEstimator}
import org.apache.spark.streaming.rdd.WriteAheadLogBackedBlockRDD
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.wal.WriteAheadLogUtils

import scala.reflect.ClassTag

/**
	* [[InputDStream]]的抽象类用于获取Receiver实例并发送到Worker上运
	* 行以便接收数据。通过`getReceiver()`方法获取接收数据的实例。
	*
	* @param ssc_ 用来执行当前DStream的上下文
	* @tparam T 当前DStream中元素的类型
	*/
abstract class ReceiverInputDStream[T: ClassTag](ssc_ : SContext) extends InputDStream[T](ssc_) {

	private[streaming] class ReceiverRateController(id: Int, estimator: RateEstimator) extends RateController(id, estimator){
		/** 对外发布速率的接口 */
		override protected def publish(rate: Long): Unit = {
			ssc.scheduler.receiverTracker.sendRateUpdate(id, rate)
		}
	}

	/**
		* 若配置了backpressure机制，创建RateController动态感知数据流速。
		*
		* 最新感知的速度采用异步方式通过ReceiverTracker发送给Receiver。
		*/
	override protected[streaming] val rateController: Option[RateController] =  {
		if (RateController.isBackPressureEnabled(ssc.conf)) {
			Some(new ReceiverRateController(inputDStreamId, RateEstimator.create(ssc.conf, ssc.graph.batchDuration)))
		} else None
	}

	/**
		* 任何[[ReceiverInputDStream]]的实现类都需要实现该方法，用于获
		* 取Receiver实例，然后发送到Worker节点上运行实时接收数据。
		* @return
		*/
	def getReceiver(): Receiver[T]

	override def start(): Unit = {}
	override def stop(): Unit = {}


	/**
		* 根据Receiver接收数据创建的Blocks生成RDD
		*/
	override def compute(validTime: Time): Option[RDD[T]] = {
		val blockRDD = {
			if (validTime < graph.startTime) {
				/**
					* 当Driver失败重启时，若没有启用WAL机制就无法恢复数据，因此
					* 直接返回空的RDD。
					*/
				new BlockRDD[T](ssc.sc, Array.empty)
			} else {
				/**
					* 向ReceiverTracker获取当前批次所有产生的Blocks
					*/
				val receivierTracker = ssc.scheduler.receiverTracker
				val blockInfos = receivierTracker.getBlocksOfBatch(validTime).getOrElse(inputDStreamId, Seq.empty)

				val inputInfo = StreamInputInfo(inputDStreamId, blockInfos.flatMap(_.numRecords).sum)
				ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)

				//创建BlockRDD
				createBlockRDD(validTime, blockInfos)
			}
		}
		Some(blockRDD)
	}

	private[streaming] def createBlockRDD(time: Time, blockInfos: Seq[ReceivedBlockInfo]):RDD[T] =  {
		if(blockInfos.nonEmpty) {
			val blockIds = blockInfos.map(_.blockId.asInstanceOf[BlockId]).toArray

			val areWALRecordHandlesPresent = blockInfos.forall{_.walRecordHandleOption.nonEmpty}

			if (areWALRecordHandlesPresent) {
				//若所有的Block都有WAL Record Handle则创建WriteAheadLogBackedBlockRDD
				val isBlockValid = blockInfos.map(_.isBlockIdValid()).toArray
				val walRecordsHandles = blockInfos.map(_.walRecordHandleOption.get).toArray
				new WriteAheadLogBackedBlockRDD[T](ssc.sc,blockIds, walRecordsHandles,isBlockValid)
			} else {
				//创建BlockRDD

				if(blockInfos.find(_.walRecordHandleOption.nonEmpty).nonEmpty) {
					if (WriteAheadLogUtils.enableReceiverLog(ssc.conf)) {
						logError("有些Block没有对应的WAL信息，这不是我们" +
							"所希望的，当Driver故障恢复时，这部分数据无法恢复！")
					} else {
						logError("有些Block有对应的WAL信息，这不是我们" +
							"所希望的，因为没有打开WAL机制！！")
					}
				}

				val validBlockIds = blockIds.filter{id =>
					ssc.sparkContext.env.blockManager.master.contains(id)
				}

				if(validBlockIds.size != blockIds.size) {
					logWarning("有部分数据在Executor内存中无法找到，" +
						"为避免数据丢失，建议开启WAL机制")
				}

				new BlockRDD[T](ssc.sc, blockIds)
			}
		} else {
			//若没有已准备好的Block，则根据配置创建空BlockRDD或WriteAheadLogBackedBlockRDD
			if(WriteAheadLogUtils.enableReceiverLog(ssc.conf)) {
				new WriteAheadLogBackedBlockRDD[T](ssc.sc, Array.empty, Array.empty, Array.empty)
			} else {
				new BlockRDD[T](ssc.sc, Array.empty)
			}
		}
	}

}
