package org.apache.spark.streaming.domain

import org.apache.spark.streaming.Time

case class OutputOperationInfo(
		batchTime: Time,
		id: Int,
		name: String,
		description: String,
		startTime: Option[Long],
		endTime: Option[Long],
		failureReason: Option[String]
		) {

	/**
		* 输出操作的执行时间
		*/
	def  duration = {
		for {
			s <- startTime
			e <- endTime
		} yield e - s
	}
}
