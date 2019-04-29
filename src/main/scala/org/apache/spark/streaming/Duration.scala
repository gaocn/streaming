package org.apache.spark.streaming

import org.apache.spark.util.Utils

/**
	* 时间长度，单位为毫秒
	*/
private[streaming] case class Duration(private val millis: Long) {
	def milliseconds: Long = millis
	def ==(that: Duration): Boolean = this.millis == that.millis
	def >(that: Duration): Boolean = this.millis > that.millis
	def >=(that: Duration): Boolean = this.millis >= that.millis
	def <(that: Duration): Boolean = this.millis < that.millis
	def <=(that: Duration): Boolean = this.millis <= that.millis

	def +(that: Duration): Duration = new Duration(this.millis + that.millis)
	def -(that: Duration): Duration = new Duration(this.millis - that.millis)
	def *(times: Int): Duration = new Duration(this.millis * times)
	def /(that: Duration): Double = this.millis.toDouble / that.millis.toDouble

	/**
		* 提供Java友好的接口
		*/
	def eq(that: Duration): Boolean = this.millis == that.millis
	def greater(that: Duration): Boolean = this.millis > that.millis
	def greaterEq(that: Duration): Boolean = this.millis >= that.millis
	def less(that: Duration): Boolean = this.millis < that.millis
	def lessEq(that: Duration): Boolean = this.millis <= that.millis

	def plus(that: Duration): Duration = new Duration(this.millis + that.millis)
	def minus(that: Duration): Duration = new Duration(this.millis - that.millis)
	def times(times: Int): Duration = new Duration(this.millis * times)
	def div(that: Duration): Double = this.millis.toDouble / that.millis.toDouble


	def isMultipleOf(that: Duration): Boolean = {
		(this.millis % that.millis == 0)
	}

	def min(that: Duration): Duration = if(this < that) this else that
	def max(that: Duration): Duration = if(this > that) this else that

	def isZero: Boolean = (this.millis ==  0)

	def toFormattedString:String = millis.toString

	def prettyPrint: String = Utils.msDurationToString(millis)

	override def toString: String = s"${this.millis}ms"
}

/**
	* 辅助对象，用于创建以毫秒单位的时间长度Duration实例
	*/
object Milliseconds {
	def apply(millis: Long): Duration = new Duration(millis)
}

/**
	* 辅助对象，用于创建以秒单位的时间长度Duration实例
	*/
object Seconds {
	def apply(seconds: Long): Duration = new Duration(seconds * 1000)
}

/**
	* 辅助对象，用于创建以分钟单位的时间长度Duration实例
	*/
object Minutes {
	def apply(seconds: Long): Duration = new Duration(seconds * 1000 * 60)
}

/**
	* Java友好的创建时间长度的辅助类
	*/
object Durations {
	def milliseconds(millis: Long): Duration = Milliseconds(millis)
	def seconds(seconds: Long): Duration = Seconds(seconds)
	def minutes(minutes: Long): Duration = Minutes(minutes)
}