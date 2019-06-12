package org.apache.spark.util

/**
	* 时钟处理器，streaming中使用，参见[[org.apache.spark.streaming.receiver.BlockGenerator]]
	*/
trait GClock {
	def getTimeMillis(): Long
	def waitTillTime(targetTime: Long): Long
}

/**
	* 采用真实OS系统时间的时钟
	*/
class GSystemClock extends GClock {
	val minPollTime = 25L

	/**
		* 获取OS系统时间
		* @return
		*/
	override def getTimeMillis(): Long = System.currentTimeMillis()

	/**
		* 阻塞至少到`targetTime`毫秒的时间
		* @param targetTime
		* @return 当前时间
		*/
	override def waitTillTime(targetTime: Long): Long = {
		var currentTime = 0L

		currentTime = System.currentTimeMillis()

		var waitTime = targetTime - currentTime
		if (waitTime <= 0) {
			return currentTime
		}

		val pollTime = math.max(waitTime / 10.0, minPollTime).toLong
		while (true) {
			currentTime = System.currentTimeMillis()
			waitTime = targetTime - currentTime
			if (waitTime <= 0) {
				return currentTime
			}
			val sleepTime = math.min(waitTime, pollTime)
			Thread.sleep(sleepTime)
		}
		-1L
	}
}
