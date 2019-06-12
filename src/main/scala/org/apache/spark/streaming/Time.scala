package org.apache.spark.streaming

/**
	* 表示时间的类，单位毫秒，用长整型表示从1970年1月1日 00:00开始，与
	* System.currentTimeMillis返回的格式是一样的。
	*/
case class Time(private val millis: Long) {

	def milliseconds: Long  = millis

	def >(that: Time):Boolean = this.millis > that.millis
	def >=(that: Time):Boolean = this.millis >= that.millis

	def <(that: Time):Boolean = this.millis < that.millis
	def <=(that: Time):Boolean = this.millis <= that.millis

	def +(duration: Duration): Time = new Time(this.millis + duration.milliseconds)

	def -(that: Time): Duration = new Duration(this.millis - that.millis)
	def -(duration: Duration): Time = new Time(this.millis - duration.milliseconds)

	//时间比较，对Java语言友好的版本
	def eq(that: Time):Boolean = this.millis == that.millis
	def greater(that: Time):Boolean = this.millis > that.millis
	def greaterEq(that: Time):Boolean = this.millis >= that.millis
	def less(that: Time):Boolean = this.millis < that.millis
	def lessEq(that: Time):Boolean = this.millis <= that.millis

	def plus(duration: Duration): Time = new Time(this.millis + duration.milliseconds)

	def minus(that: Time): Duration = new Duration(this.millis - that.millis)
	def minus(duration: Duration): Time = new Time(this.millis - duration.milliseconds)


	def max(that: Time): Time = if(this > that) this else that
	def min(that: Time): Time = if(this < that) this else that

	def isMultipleOf(duration: Duration): Boolean = {
		if (duration.milliseconds == 0) {
			throw new IllegalArgumentException("时长duration不能为0!")
		}
		this.millis % duration.milliseconds == 0
	}

	/**
		* 对当前时间`this.millis`进行处理确保其是`duration`整数倍，即
		* 按照`duration`长度对齐。
		*
		* 例如：
		*       this.millis = 1556438237644
		* 			duration    =         10000
		* 则floor(duration) = 1556438230000
		*
		*       this.millis = 1556438237644
		* duration    =                   4
		* 则floor(duration) = 1556438230000
		*
		*       this.millis = 1556438237644
		* duration    =                   5
		* 则floor(duration) = 1556438237640
		*
		* @param duration 时间长度
		* @return
		*/
	def floor(duration: Duration): Time = {
		val t = duration.milliseconds
		new Time((this.millis / t) * t)
	}

	/**
		* @param duration 时间长度
		* @param zeroTime 指定开始时间
		* @return
		*/
	def floor(duration: Duration, zeroTime: Time): Time = {
		val t = duration.milliseconds
		new Time(((this.millis - zeroTime.millis) / t) * t + zeroTime.millis)
	}

	/**
		* 返回从当前时间到终止时间，按照给定步长得到时间序列
		* @param that 终止时间(不包含)
		* @param interval 步长
		* @return
		*/
	def until(that: Time, interval: Duration): Seq[Time] = {
		(this.millis) until (that.millis) by (interval.milliseconds) map (new Time(_))
	}

	/**
		* 返回从当前时间到终止时间，按照给定步长得到时间序列
		* @param that 终止时间(包含)
		* @param interval 步长
		* @return
		*/
	def to(that: Time, interval: Duration): Seq[Time] = {
		(this.millis) to (that.millis) by (interval.milliseconds) map (new Time(_))
	}

	override def toString: String = s"${this.millis}ms"
}

object Time {
	/**
		* 默认按照时间递增排序
		*/
	implicit val ordering = Ordering.by((time:Time) => time.millis)
}
