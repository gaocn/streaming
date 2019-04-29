package org.apache.spark.streaming;

public enum SContextState {
	/**
	 * SContext上下文被创建，当还没有被启动
	 * 此时InputDStream，transformation和action操作可能已在该上下
	 * 文中被创建。
	 */
	INITIALIZING,

	/**
	 * SContext上下文被启动
	 * 此时InputDStream，transformation和action操作可能已在该上下
	 * 	文中被创建。
	 */
	ACTIVE,

	/**
	 * SContext被停止，并且不能在被使用
	 */
	STOPPED
}
