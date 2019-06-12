package org.apache.spark.streaming.scheduler

/**
	* Receiver的状态
	*/
object ReceiverState extends Enumeration {
	type ReceiverState = Value

	val INACTIVE, SCHEDULED, ACTIVE = Value
}
