package org.apache.spark.util

/**
	* 可以任意设置、修改的时钟，除非手动设置，否则其其内部时间不变
	*/
class GManualClock (private var time:Long) extends GClock {
	def this() = this(0L)


	override def getTimeMillis(): Long = synchronized {
		time
	}

	/**
		* 手动设置时间
		* @param timeToSet
		*/
	def setTime(timeToSet: Long): Unit = synchronized {
		time = timeToSet
		notifyAll()
	}

	/**
		* 时钟向前走 `timetoAdd` 毫秒
		* @param timetoAdd
		*/
	def advance(timetoAdd: Long):Unit = synchronized {
		time += timetoAdd
		notifyAll()
	}

	/**
		* @param targetTime 阻塞，直到时钟要设置或advance走到至少该时间
		* @return 等待结束后，返回当前时间
		*/
	override def waitTillTime(targetTime: Long): Long = {
		while (time < targetTime) {
			wait(10)
		}
		getTimeMillis()
	}
}
