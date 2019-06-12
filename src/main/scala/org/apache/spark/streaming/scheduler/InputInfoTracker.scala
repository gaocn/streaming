package org.apache.spark.streaming.scheduler

import org.apache.spark.Logging
import org.apache.spark.streaming.{SContext, Time}
import org.apache.spark.streaming.domain.StreamInputInfo

import scala.collection.mutable

/**
	* 用于保存所有输入流及其所有输入数据的统计信息，这些数据会通过[[SListener]]
	* 对外暴露以便监控
	*/
class InputInfoTracker(ssc: SContext) extends Logging{

	private val batchTimeToInputInfo  = new mutable.HashMap[Time, mutable.HashMap[Int, StreamInputInfo]]()


	/** 将input stream信息报告给tracker */
	def reportInfo(batchTime: Time, inputInfo:StreamInputInfo): Unit = synchronized{
		val inputInfos = batchTimeToInputInfo.getOrElseUpdate(batchTime,new mutable.HashMap[Int, StreamInputInfo]())

		if(inputInfos.contains(inputInfo.inputStreamId)) {
			throw new IllegalStateException(s"${inputInfo.inputStreamId}在${batchTime}时刻的" +
				s"batch已经被添加，当前操作是非法！")
		}

		inputInfos += inputInfo.inputStreamId -> inputInfo
	}

	def getInfo(batchTime:Time):Map[Int, StreamInputInfo] = synchronized{
		val inputInfos = batchTimeToInputInfo.get(batchTime)
		//将mutable HashMap转换为immutable
		inputInfos.map(_.toMap).getOrElse(Map[Int, StreamInputInfo]())
	}

	/** 清理所有在 batchThreshTime之前的旧的信息*/
	def cleanup(batchThreshTime: Time): Unit = synchronized{
		val timesToCleanup = batchTimeToInputInfo.keys.filter(_ < batchThreshTime)
		logInfo(s"清理旧的batch metadata：${timesToCleanup.mkString(" ")}")
		batchTimeToInputInfo --= timesToCleanup
	}
}
