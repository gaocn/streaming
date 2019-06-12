package org.apache.spark.streaming.receiver

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.rate.RateLimiter
import org.apache.spark.util.{GClock, GSystemClock, RecurringTimer}
import org.apache.spark.util.{GSystemClock, RecurringTimer}

import scala.collection.mutable.ArrayBuffer

/**
	* 将[[Receiver]]接收到的数据按照时间间隔(block interval)产生一批
	* 批对象，并将它们放到不同的blocks中。
	*
	* 当前类实例会启动两个线程：
	* 	1、第一个线程，周期性产生一个批次，并将前一个批次作为一个block；
	* 	2、第二个线程，将产生的blocks存储到BlockManager中；
	*
	* NOTE：不要在[[Receiver]]内部创建[[BlockGenerator]]实例，通过
	* ReceiverSupervisor.createBlockGenerator创建并使用。
	*
	* [[ReceiverSupervisor]]是在
	*
	*/
private[streaming] class BlockGenerator(
			 listener:BlockGeneratorListener,
			 streamId:Int,
			 conf:SparkConf,
			 clock:GClock = new GSystemClock())
				//通过限定写入速度来限定数据流入速度
	     extends RateLimiter(conf) with Logging {

	/** Block，每次存储BlockManger的基本单元 */
	private case class Block(id: StreamBlockId, buffer: ArrayBuffer[Any])


	/**
		* BlockGenerator的5中状态：
		* 1、Initialized，尚未启动；
		* 2、Active，start()方法被调用，正在根据接收到的数据产生Blocks；
		* 3、StoppedAddingData，stop()方法被调用，停止添加接收的数据，
		* 		但Blocks仍在产生并且存储BlockManager中。
		* 4、StoppedGeneratingBlocks，停在生产Blocks，但Blocks仍在被
		* 		存储到BlockManager中。
		* 5、StoppedAll，停止所有过程，BlockGenerator实例可以被GC！
		*/
	private object GeneratorState extends Enumeration{
		type GeneratorState = Value

		val Initialized, Active, StoppedAddingData, StoppedGeneratingBlocks, StoppedAll = Value
	}
	import GeneratorState._

	/**
		* 多长时间产生一个Block，一个Block对应RDD中的一个Partition。
		* 默认情况下，1s产生5个Block，实际经验blockInterval不能低于50ms，
		* 否则一个Task计算的数据太少，且写磁盘频繁，效率不高。！
		*/
	private val blockIntervalMs = conf.getTimeAsMs("spark.streaming.blockInterval","200ms")
	require(blockIntervalMs > 0, "spark.streaming.blockInterval值必须为正数！")

	/** 启动定时器`线程`周期执行 */
	private val blockIntervalTimer = new RecurringTimer(clock, blockIntervalMs, updateCurrentBufffer, "BlockGenerator")

	private val blockQueueSize = conf.getInt("spark.streaming.blockQueueSize", 10)
	/** 队列中等待写入磁盘中的数据 */
	private val blocksForPushing = new ArrayBlockingQueue[Block](blockQueueSize)

	/** 将Block持久化到BlockManager的线程 */
	private val blockPushingThread = new Thread(){
		override def run(): Unit = keepPushBlocks()
	}

	@volatile private var state = Initialized
	@volatile private var currentBuffer = new ArrayBuffer[Any]

	/**
		* 启动block生产线程、block存储线程
		*/
	def start(): Unit = synchronized{
		if (state == Initialized) {
			state = Active
			blockIntervalTimer.start()
			blockPushingThread.start()
			logInfo("启动BlockGenerator")
		} else {
			throw new Exception(s"当前BlockGenerator实例转为为${state}，不是Initialized状态无法启动！")
		}
	}

	/**
		* 按顺序停止线程，确保多有已添加的数据能够被持久化。
		* 1、停止向currentBuffer中添加数据；
		* 2、停止生成blocks；
		* 3、等到所有`blocksForPushing`队列中的blocks持久化后结束；
		*/
	def stop(): Unit = {
		synchronized {
			if (state == Active) {
				state = StoppedAddingData
			} else {
				logWarning("BlockGenerator不是Active状态，无法停止")
				return
			}
		}

		logInfo("正在停止BlockGenerator....")
		//停止生产blocks，
		blockIntervalTimer.stop(false)
		synchronized{state = StoppedGeneratingBlocks}

		logInfo("等待队列中的所有block持久化后再停止push thread")
		blockPushingThread.join()
		synchronized{state = StoppedAll}

		logInfo("已停止BlockGenerator")
	}

	def isActive(): Boolean = state == Active
	def isStopped(): Boolean = state == StoppedAll

	/** 添加一条数据到缓冲区currentBuffer中 */
	def addData(data: Any): Unit = {
		if (state == Active) {
			waitToPush()
			synchronized{
				if (state == Active) {
					currentBuffer += data
				} else {
					throw new Exception("BlockGenerator状态不是Active或已关闭，不能添加数据")
				}
			}
		} else {
			throw new Exception("BlockGenerator状态不是Active或已关闭，不能添加数据")
		}
	}

	/**
		* 当成功添加数据到缓存区后，调用BlockGeneratorListener.onAddData
		* 回调方法
		*/
	def addDataWithCallback(data: Any, metadata: Any): Unit = {
		if (state == Active) {
			waitToPush()
			synchronized{
				if (state == Active) {
					currentBuffer += data
					listener.onAddData(data, metadata)
				} else {
					throw new Exception("BlockGenerator状态不是Active或已关闭，不能添加数据")
				}
			}
		} else {
			throw new Exception("BlockGenerator状态不是Active或已关闭，不能添加数据")
		}
	}

	/**
		* 当成功添加多条数据到缓存区后，调用BlockGeneratorListener.onAddData
		* 回调方法。
		*
		* 所有数据原子地加入缓存区中，并且保证这些数据被保存在同一个block中！
		*
		*/
	def addMultipleDataWithCallback(dataIter: Iterator[Any], metadata: Any): Unit = {
		if (state == Active) {
			val tmpBuffer = new ArrayBuffer[Any]
			dataIter.foreach(elem => {
				waitToPush()
				tmpBuffer += elem
			})

			synchronized{
				if (state == Active) {
					currentBuffer ++= tmpBuffer
					listener.onAddData(tmpBuffer, metadata)
				} else {
					throw new Exception("BlockGenerator状态不是Active或已关闭，不能添加数据")
				}
			}
		} else {
			throw new Exception("BlockGenerator状态不是Active或已关闭，不能添加数据")
		}
	}

	/** 通过调用BlockGeneratorListener.onError回调报告错误 */
	private def reportError(msg:String, cause: Throwable): Unit = {
		logError(msg, cause)
		listener.onError(msg, cause)
	}

	/** 调用BlockGeneratorListener.onPushBlock方法持久化block */
	private def pushBlock(block: Block): Unit = {
		listener.onPushBlock(block.id, block.buffer)
		logInfo(s"开始持久化block=${block.id}")
	}

	/**
		* 接收到一条数据放到缓存中，然后把缓存中的内容按照时间或尺寸合并为
		* block，利用定时器将数据不断的合并为block。
		*
		* 数据不断进入addData和数据不断合并为block两个操作是分别在两个线
		* 程中完成的！
		*
		* @param time
		*/
	def updateCurrentBufffer(time: Long): Unit = {
		try {
			var newBlock: Block = null
			synchronized {
				if (currentBuffer.nonEmpty) {
					val newBlockBuffer = currentBuffer
					currentBuffer = new ArrayBuffer[Any]
					val blockId = StreamBlockId(streamId, time - blockIntervalMs)
					listener.onGenerateBlock(blockId)
					newBlock = Block(blockId, newBlockBuffer)
				}
			}

			//若队列慢了，则阻塞
			if (newBlock != null) {
				blocksForPushing.put(newBlock)
			}
		} catch {
			case ie: InterruptedException =>
				logInfo("生产Block的定时器被中断 ")
			case e: Exception =>
				reportError("生产Block的定时器内部出错", e)
		}

	}

	/** 将产生的block持久化到BlockManager中 */
	def keepPushBlocks(): Unit = {
		logInfo("启动Block持久化线程")
		def areBlocksBeingGenerated:Boolean =synchronized{
			state != StoppedGeneratingBlocks
		}

		try {
			while (areBlocksBeingGenerated) {
				//若blocks队列不为空，则将block持久化到BlockManager中
				Option(blocksForPushing.poll(10, TimeUnit.MICROSECONDS)) match {
					case Some(block) => pushBlock(block)
					case None =>
				}
			}

			//到这说明已停止产生Blocks，只需要将队列中所有的block持久化到BlockManger中
			logInfo(s"已终止产生block，下面把队列中的已生产的block[size=${blocksForPushing.size()}]持久化到BlockManger中")
			while (!blocksForPushing.isEmpty) {
				val block = blocksForPushing.take()
				logInfo(s"准备将block=${block}持久化")
				pushBlock(block)
				logInfo(s"block队列中还剩余size=${blocksForPushing.size()}个")
			}

			logInfo("停止block pushing线程")
		} catch {
			case ie: InterruptedException =>
				logInfo(s"block pushing线程被中断：${ie.getMessage}")
			case e: Exception =>
				reportError("block pushing线程内部出错", e)
		}

	}
}
