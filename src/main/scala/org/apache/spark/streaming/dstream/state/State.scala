package org.apache.spark.streaming.dstream.state


/**
	* 用于实现`mapWithState`中获取、更新状态！
	*/
abstract class State[S] {

	def exists(): Boolean

	/**
		* 1、首先检查状态是否存在，若不存在则抛出NoSuchElementException；
		* 2、若状态存在，则直接返回；
		*/
	def get(): S

	/**
		* 若状态不存在或已被移除，则会抛出IllegalArgumentException
		*/
	def update(newState: S): Unit

	/**
		* 若状态存在，则移除
		*/
	def remove(): Unit

	/**
		* 当前batch后，state将会超时或被移除。
		* 当StateSpec中指定的超时时间并且超时时间内没有更新状态就会state移除。
		*/
	def isTimingOut():Boolean

  @inline	final def getOption(): Option[S] = if (exists()) Some(get()) else None

	@inline final override def toString: String = {
		getOption().map{_.toString}.getOrElse("<state not set>")
	}
}
