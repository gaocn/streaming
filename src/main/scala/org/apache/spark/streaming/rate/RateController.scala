package org.apache.spark.streaming.rate

import java.io.ObjectInputStream
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.streaming.scheduler.{SListener, SListenerBatchCompleted}
import org.apache.spark.util.ThreadUtils

import scala.concurrent.{ExecutionContext, Future}

/**
	* Streaming监听器，在每次批次作业执行完毕后，评估当前流的速度，以实
	* 现对流输入速度的控制。
	*
	* 实现类：
	*		ReceiverRateController
	*/
private[streaming] abstract class RateController(val streamUID:Int, rateEstimator: RateEstimator ) extends SListener with Serializable with Logging{
	init()

	@transient
	implicit private var executionContext:ExecutionContext = _

	@transient
	private var rateLimit: AtomicLong = _


	private def init(): Unit = {
		executionContext = ExecutionContext.fromExecutor(
			ThreadUtils.newDaemonSingleThreadExecutor("stream-rate-update")
		)
		rateLimit = new AtomicLong(-1L)
	}

	/**  对外发布速率的接口 */
	protected def publish(rate:Long):Unit

	private def  readObject(ois: ObjectInputStream):Unit = {
		try {
			ois.defaultReadObject()
			init()
		} catch {
			case e:Exception =>
				logWarning("反序列化异常")
		} finally {
			if(ois != null) {
				ois.close()
			}
		}
	}

	private def computeAndPublish(time: Long, elems: Long, workDelay:Long, waitDelay:Long): Unit = Future[Unit]{
		val newRate = rateEstimator.compute(time, elems, workDelay, waitDelay)
		newRate.foreach{s =>
			rateLimit.set(s.toLong)
			publish(getLatestRate())
		}
	}

	def getLatestRate():Long = rateLimit.get()

	override def onBatchCompleted(batchCompleted: SListenerBatchCompleted): Unit = {
		val elements = batchCompleted.batchInfo.streamIdtoInputInfo

		for{
			processingEnd <- batchCompleted.batchInfo.processingEndTime
			workDelay <- batchCompleted.batchInfo.processingDelay
			waitDelay <- batchCompleted.batchInfo.schedulingDelay
			elems  <- elements.get(streamUID).map(_.numRecords)
		} computeAndPublish(processingEnd, elems,workDelay, waitDelay)
	}
}

object RateController {
	def isBackPressureEnabled(conf:SparkConf):Boolean = {
		conf.getBoolean("spark.streaming.backpressure.enabled", false)
	}
}