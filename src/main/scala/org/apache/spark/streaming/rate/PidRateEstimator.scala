package org.apache.spark.streaming.rate

import org.apache.spark.Logging

/**
	* proportional-integral-derivative (PID)的实现用于Streaming
	* 中的速度评估和控制。
	*
	* PIDRateEstimtor中的误差为：
	* 		measured processing rate - previous rate
	* 其中当前的处理速度为：numElements/processingDelay
	*
	* @see https://en.wikipedia.org/wiki/PID_controller
	* @param batchInterval 采样周期为BatchInterval
	* @param proportional 比例，修正值依赖于误差的比例，太大会导致结
	*                     果超过最大值，太小导致控制器不太敏感。
	*                     默认值为1。
	* @param integral 积分参数，修正值依赖过去累积误差的比例，用于降
	*                 低修正误差，可以让修正值尽可能接近真实值。
	*                 默认值为0.2
	* @param derived  微分参数，修正值依赖未来预测误差的比例，因为会
	*                 影响系统的稳定性，很少使用，除非系统具有很大的滞
	*                 后性。
	*                 默认为值为0。
	* @param minRate  最小流入速率，确保有数据被Receiver接收
	*/
class PidRateEstimator(batchInterval: Long, proportional: Double, integral: Double, derived: Double, minRate: Double) extends RateEstimator with Logging{
	private var firstRun:Boolean = true
	private var latestTime: Long = -1L
	private var latestRate: Double = -1D
	private var lastestError:Double =  -1D


	require(batchInterval >  0, "PidRateEstimator中的采样周期必须大于0")
	require(proportional >= 0, "PidRateEstimator中的比例参数必须大于0")
	require(integral >= 0, "PidRateEstimator中的积分参数必须大于0")
	require(derived >= 0, "PidRateEstimator中的微分必须大于0")
	require(minRate > 0, "PidRateEstimator中的minRate必须大于0")


	/**
		* 根据每次Batch完成时间信息以及数据量，计算当前流每秒处理的数据量。
		*
		* @param time            当前批次完成的时间
		* @param numElemtens        当前批次处理的元素个数
		* @param processingDelay 当前批次处理延迟
		* @param schedulingDelya 当前批次的调度延迟(在队列中的时间)
		* @return
		*/
	override def compute(time: Long, numElemtens: Long, processingDelay: Long, schedulingDelya: Long): Option[Double] = {
		logTrace(s"\n时间：${time}，处理记录数：${numElemtens}， 处理延迟：${processingDelay}，调度延迟：${schedulingDelya}\n")

		this.synchronized{
			if(time >  0 && numElemtens > 0 && processingDelay > 0) {
				//计算举例上次更新的时间间隔，单位秒
				val delaySinceUpdate = (time - latestTime).toDouble / 1000

				//计算实际处理速度
				val processingRate = numElemtens.toDouble / processingDelay * 1000

				//计算当前误差
				val error = latestRate - processingRate

				//计算积分误差
				//累积误差是根据调度延迟计算，调度延迟没延迟N秒就会导致N*processRate
				// 个记录不能在上一次批次中被处理。要计算积分误差就是要计算这N*processRate
				// 条记录应该按照哪个速度被处理，这个应该被处理的速度就是累积误差。
				val historicalError = schedulingDelya.toDouble * processingRate / batchInterval

				//计算微分误差
				val derror = (error - lastestError) / delaySinceUpdate

				val newRate = (latestRate - proportional * error -
																				integral * historicalError -
																				derived * derror).max(minRate)

				logInfo(
					s"""
						 |latestRate=${latestRate}，error=${error}，
						 |latestError=${lastestError}，historicalError=${historicalError}，
						 |delaySinceUpdate=${delaySinceUpdate}，derror=${derror}
					 """.stripMargin)

				latestTime = time
				if (firstRun) {
					latestRate = processingRate
					lastestError = 0D
					firstRun =  false
					logInfo("PIDRateEstimator首次运行，跳过...")
					None
				} else {
					latestRate = newRate
					lastestError = error
					logInfo(s"PIDRateEstimator运行，计算修正后的速度为：${newRate}")
					Some(newRate)
				}
			} else {
				logTrace("条件不满足，跳过PID速度预测")
				None
			}
		}
	}
}
