package org.apache.spark.streaming

/**
	* 时间间隔，有起始时间
	*/
class Interval(val beginTime: Time, val endTime: Time) {
	def this(beginMS: Long, endMs: Long) = this(new Time(beginMS), new Time(endMs))

	def duration: Duration = endTime - beginTime

	def +(duration: Duration): Interval = {
		new Interval(beginTime + duration, endTime + duration)
	}

	def -(duration: Duration): Interval = {
		new Interval(beginTime - duration, endTime - duration)
	}

	def ==(that: Interval):Boolean = {
		if(this.duration != that.duration) {
			throw new Exception(s"比较两个不同时间长度的时间间隔：${this}，${that}")
		}
		this.endTime == that.endTime
	}

	def <(that: Interval):Boolean = {
		if(this.duration != that.duration) {
			throw new Exception(s"比较两个不同时间长度的时间间隔：${this}，${that}")
		}
		this.endTime < that.endTime
	}

	def <=(that: Interval): Boolean = {
		this < that || this == that
	}

	def >(that: Interval):Boolean =  {
		!(this <= that)
	}

	def >=(that: Interval): Boolean = {
		!(this < that)
	}

	override def toString: String = s"[${beginTime},${endTime}]"
}

object Interval {
	/**
		* 返回从当前时刻导致指定时间长度的时间间隔，开始时间按照指定
		* duration对齐
		* @param duration
		* @return
		*/
	def currentInterval(duration: Duration): Interval  = {
		val time = new Time(System.currentTimeMillis())
		val intervalBegin = time.floor(duration)
		new Interval(intervalBegin, intervalBegin + duration)
	}
}