package org.apache.spark.streaming.rate

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Duration

/**
	* 根据每次Batch完成时的更新事件，评估InputDStream输入速度的组件。
	*/
private[streaming] trait RateEstimator extends Serializable {

	/**
		* 根据每次Batch完成时间信息以及数据量，计算当前流每秒处理的数据量。
		* @param time 当前批次完成的时间
		* @param elemtens 当前批次处理的元素个数
		* @param processingDelay 当前批次处理延迟
		* @param schedulingDelya 当前批次的调度延迟(在队列中的时间)
		* @return
		*/
	def compute(
			 time: Long,
			 elemtens:Long,
			 processingDelay:Long,
			 schedulingDelya:Long):Option[Double]
}

object RateEstimator {
	/**
		*  目前仅仅支持`pid`速度评估器
		*
		* @return RateEstimator的实例
		*/
	def create(conf:SparkConf, batchInterval:Duration):RateEstimator = {
		conf.get("spark.streaming.backpressure.rateEstimator", "pid") match {
			case "pid" =>
				val proportional = conf.getDouble("spark.streaming.backpressure.pid.proportional",1.0)
				val integral = conf.getDouble("spark.streaming.backpressuce.pid.integral", 0.2)
				val derived = conf.getDouble("spark.streaming.backpressuce.pid.derived", 0.0)
				val minRate = conf.getDouble("spark.streaming.backpressuce.pid.minRate", 100)

				new PidRateEstimator(batchInterval.milliseconds, proportional, integral, derived, minRate)

			case estimator =>
				throw new IllegalArgumentException(s"不支持的速度评估器：${estimator}")
		}
	}
}