package org.apache.spark.streaming.graph

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.input.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.scheduler.Job
import org.apache.spark.streaming.{Duration, SContext, Time}
import org.apache.spark.util.Utils

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
	* 执行计划，有向无环图，包含数据的输入、输出。
	*
	* 根据DStream的依赖关系构建出RDD的DAG。
	*/
final class DSGraph extends Serializable with Logging{

	/** 数据输入，通过多个输入流实现，Receiver与InputDStream是一一对应 */
	private val inputDStream = new ArrayBuffer[InputDStream[_]]()
	/** 数据输出，通常是将处理后的数据输出高外部存储，如Kafka、Redis、MySQL、HBase等 */
	private val outputDStream = new ArrayBuffer[DStream[_]]()

	/** 实例化后的RDD应该保存的间隔 */
	var rememberDuration:Duration = null
	var checkpointInProgress = false

	var zeroTime:Time = null
	var startTime:Time = null
	var batchDuration:Duration = null

	def start(time:Time): Unit = {
		this.synchronized{
			logInfo("启动DGraph，开始进行计算！")
			//require(zeroTime == null, "DStream Graph计算过程已经开始！")
			zeroTime = time
			startTime = time
			outputDStream.foreach(_.initialize(time))
			outputDStream.foreach(_.remember(rememberDuration))
			outputDStream.foreach(_.validateAtStart())
			inputDStream.par.foreach(_.start())
		}
	}

	def restart(time:Time): Unit = {
		this.synchronized{
			startTime = time
		}
	}

	def stop(): Unit = {
		this.synchronized {
			inputDStream.foreach(_.stop())
		}
	}

	def addInputDStream[T:ClassTag](in: InputDStream[T]) = {
		this.synchronized{
			in.setGraph(this)
			inputDStream += in
		}
	}

	def addOutputStream[T:ClassTag](out: DStream[T]) = {
		this.synchronized{
			out.setGraph(this)
			outputDStream += out
		}
	}

	def getInputStreams(): Array[InputDStream[_]] = this.synchronized{
		inputDStream.toArray
	}

	def getOutputStream():Array[DStream[_]] = this.synchronized{
		outputDStream.toArray
	}

	def getInputStreamName(streamId: Int): Option[String] = synchronized{
		inputDStream.find(_.inputDStreamId == streamId).map(_.name)
	}

	def getReceiverInputStreams():Array[ReceiverInputDStream[_]] = this.synchronized{
		inputDStream.filter(_.isInstanceOf[ReceiverInputDStream[_]])
  		.map(_.asInstanceOf[ReceiverInputDStream[_]])
  		.toArray
	}

	def validate(): Unit = {
		logInfo("开始对DSGraph进行验证....")
		this.synchronized{
			require(batchDuration != null, "没有设置Batch Duration")

			require(getOutputStream().size > 0, "没有OutputStream操作被注册，因此不会执行")
		}
	}

	def setContext(ssc: SContext): Unit = {
		logInfo("设置DSGraph中所有OutputDStream中的SContext")
		this.synchronized{
			outputDStream.foreach(_.setContext(ssc))
		}

	}

	def setBatchDuration(batchDur: Duration):Unit = {
		logInfo(s"设置DSGraph中的duration为：${batchDur}")
		this.synchronized{
			require(batchDur != null, "流处理应用必须指定Batch Duration")
			this.batchDuration = batchDur
		}
	}

	def remember(duration: Duration): Unit = {
		logInfo(s"设置DSGraph中的remember duration为：${duration}")
	}

	def clearMetadata(time:Time): Unit = {
		logInfo(s"在${time}时刻，开始清理旧的元数据")
		this.synchronized{
			outputDStream.foreach(_.clearMetadata(time))
		}
		logInfo(s"在${time}时刻，成功清理旧的元数据")
	}

	def clearCheckpointData(time: Time): Unit = {
		logInfo(s"在${time}时刻，开始清理旧的checkpoint数据")
		this.synchronized{
			outputDStream.foreach(_.clearCheckpointData(time))
		}
		logInfo(s"在${time}时刻，成功清理旧的checkpoint数据")
	}

	def updateCheckpointData(time:Time): Unit = {
		logInfo("在${time}时刻，开始更新checkpoint数据")
		this.synchronized{
			outputDStream.foreach(_.updateCheckpointData(time))
		}
		logInfo("在${time}时刻，成功更新checkpoint数据")
	}

	def restoreCheckpointData() = {
		logInfo("在${time}时刻，开始恢复checkpoint数据")
		this.synchronized{
			outputDStream.foreach(_.restoreCheckpointData())
		}
		logInfo("在${time}时刻，成功恢复checkpoint数据")
	}

	/**
		* 获取所有InputDStream中的rememberDuration最大的Duration，在
		* 执行清理操作时，需要根据该最大rememberDuration进行清理。
		*/
	def getMaxInputStreamRememberDuration():Duration = {
		//忽略rememberDuration为空的InputDStream
		inputDStream.map(_.rememberDuration).filter(_ != null).maxBy(_.milliseconds)
	}

	/*===================
	 * 生成作业
	 *===================
	 */
	def generateJob(time: Time):Seq[Job] = {
		logInfo(s"在${time}时刻，开始生成Streaming Jobs")
		val jobs = this.synchronized{
			outputDStream.flatMap{out =>
				val jobOption = out.generateJob(time)
				jobOption.foreach(_.setCallSite(out.creationSite))
				jobOption
			}
		}
		logInfo(s"在${time}时刻，产生了${jobs.length}个作业")
		jobs
	}

	/*===================
	 * 序列化、反序列化方法
	 *===================
	 */
	@throws(classOf[IOException])
	private def writeObject(oos:ObjectOutputStream): Unit = Utils.tryOrIOException{
		logInfo("DSGraph.writeObject 方法被调用")
		this.synchronized{
			checkpointInProgress = true
			logInfo("开启checkpoint模式")
			oos.defaultWriteObject()
			checkpointInProgress = false
			logInfo("关闭checkpoint模式")
		}
	}

	@throws(classOf[IOException])
	private def readObject(ois:ObjectInputStream):Unit = Utils.tryOrIOException{
		logInfo("DSGraph.readObject 方法被调用")
		this.synchronized{
			checkpointInProgress =  true
			logInfo("开启checkpoint模式")
			ois.defaultReadObject()
			checkpointInProgress = false
			logInfo("关闭checkpoint模式")
		}
	}
}
