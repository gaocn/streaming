package org.apache.spark.streaming.domain

import org.apache.spark.streaming.Time

/**
	* 已完成批量作业的信息
 *
	* @param batchTime Time of Batch
	* @param submissionTime 当前batch作业提交给scheduler队列时的时间
	* @param processingStartTime 当前batch的第一个作业被处理的时间
	* @param processingEndTime 当前batch的最后一个作业被处理的时间
	* @param streamIdtoInputInfo streamid与输入信息的映射
	* @param outputOperationInfos 输出信息
	*/
case class BatchInfo(
		batchTime: Time,
		submissionTime: Long,
		processingStartTime: Option[Long],
		processingEndTime: Option[Long],
		streamIdtoInputInfo: Map[Int, StreamInputInfo],
		outputOperationInfos: Map[Int, OutputOperationInfo]
		) {

	def schedulingDelay: Option[Long] = {
		processingStartTime.map(_ - submissionTime)
	}

	def processingDelay:Option[Long] = {
		for(e <- processingEndTime; s <- processingStartTime)
			yield e - s
	}

	def totalDelay:Option[Long] = {
		for (sd <- schedulingDelay; pd <- processingDelay)
			yield sd + pd
	}

	/**
		* 当前batch接收的记录数
		* @return
		*/
	def numRecords: Long = {
		streamIdtoInputInfo.map(_._2.numRecords).sum
	}
}

object BatchInfo {
	def main(args: Array[String]): Unit = {
		var pd:Option[Long] = Some(1000L)
		var sd:Option[Long] = Some(1200L)

		println(cal(pd, sd))
		pd =None
		println(cal(pd, sd))

	}

	def cal(processingDelay: Option[Long], schedulingDelay: Option[Long]): Option[Long] = {
		for (sd <- schedulingDelay; pd <- processingDelay)
			yield sd + pd
	}
}
