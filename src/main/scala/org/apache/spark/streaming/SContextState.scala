package org.apache.spark.streaming

object SContextState extends Enumeration {
	type SContextState = Value

	/**
		* SContext上下文被创建，当还没有被启动
		* 此时InputDStream，transformation和action操作可能已在该上下
		* 文中被创建。
		*/
	val INITIALIZING = Value(0)

	/**
		* SContext上下文被启动
		* 此时InputDStream，transformation和action操作可能已在该上下
		* 文中被创建。
		*/
	val ACTIVE = Value(1)

	/**
		* SContext被停止，并且不能在被使用
		*/
	val STOPPED  = Value(2)

}
