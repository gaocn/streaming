package org.apache.spark.streaming.domain

/**
	* 接收器的基本信息
	*/
case class ReceiverInfo(
			streamId: Int,
			name: String,
			active: Boolean,
			location: String,
			executorId: String,
			lastErrorMessage: String = "",
			lastError: String = "",
			lastErrorTime: Long = -1L
		 )