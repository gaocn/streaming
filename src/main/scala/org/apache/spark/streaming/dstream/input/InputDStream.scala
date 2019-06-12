package org.apache.spark.streaming.dstream.input

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDDOperationScope
import org.apache.spark.streaming.{Duration, SContext, Time}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.rate.RateController
import org.apache.spark.util.Utils

import scala.reflect.ClassTag

/**
	*
	*/
abstract class InputDStream[T: ClassTag](ssc_ : SContext) extends DStream[T](ssc_) {
	private[streaming] var lastValidTime: Time = null

	ssc.graph.addInputDStream(this)

	//输入流的唯一标识
	val inputDStreamId = ssc.getNewInputStreamId()

	//TODO
	protected[streaming] val rateController: Option[RateController] = None

	/**
		* 返回human readable的输入流名，例如：
		* FlumePollingDStream   ->  Flume polling stream
		*
		* @return
		*/
	private[streaming] def name: String = {
		val newName = Utils.getFormattedClassName(this)
			.replaceAll("InputDStream", "Stream")
			.split("(?=[A-Z])")
			.filter(_.nonEmpty)
			.mkString(" ")
			.toLowerCase
			.capitalize
		s"${newName} [${inputDStreamId}]"
	}

	/**
		* 用于启动、关闭接收数据进程的方法，子类需要实现
		*/
	def start()
	def stop()


	/** 当前DStream实例的所有父亲DStream列表 */
	override def dependencies: List[DStream[_]] = List()

	/** DStream生成RDD的时间间隔 */
	override def slideDuration: Duration = {
		if (ssc == null) throw new Exception("ssc为空！")
		if (ssc.graph.batchDuration == null) throw new Exception("batchDuratin为空！")
		ssc.graph.batchDuration
	}

	/**
		* 确保`time`为slideDuration的整数倍，同时time是严格递增的，以
		* 保证InputDStream.compute()方法 一直在时间递增时被调用
		*/
	override private[streaming] def isTimeValid(time: Time): Boolean = {
		if (!super.isTimeValid(time)) {
			false
		} else {
			//time有效，下面检查是否要大于`lastValidTime`
			if (lastValidTime != null && time < lastValidTime) {
				logWarning(s"isTimeVliad方法被调用，当前时间为：" +
					s"${time}，上一次的有效时间为：${lastValidTime}")
			}
			lastValidTime = time
			true
		}
	}

	/**
		* 当前流的操作所在的base scope，对于InputDStream，scope为类名，
		* 对于其他outer scope我哦们假定其包含一个scope name
		*/
	override protected[streaming] val baseScope:Option[String]= {
		val scopeName = Option(ssc.sc.getLocalProperty(SparkContext.RDD_SCOPE_KEY))
  		.map(json=> RDDOperationScope.fromJson(json).name + s"[${inputDStreamId}]")
  		.getOrElse(name.toLowerCase)
		Some(new RDDOperationScope(scopeName).toJson)
	}
}
