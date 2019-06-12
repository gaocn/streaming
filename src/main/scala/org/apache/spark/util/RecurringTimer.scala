package org.apache.spark.util

import org.apache.spark.Logging

/**
	* 定期器，每隔period毫秒调用一次callback函数
	* @param clock
	* @param period
	* @param callback
	* @param name
	*/
class RecurringTimer(clock:GClock, period: Long, callback:(Long)=>Unit, name:String) extends Logging{

	private val thread = new Thread(s"定时器-${name}") {
		setDaemon(true)
		override def run(): Unit = {loop}
	}

	@volatile private var prevTime = -1L
	@volatile private var nextTime = -1L
	@volatile private var stopped = false

	/**
		* 每隔period时间调用callback一次
		*/
	def loop(): Unit = {
		try {
			while (!stopped) {
				triggerActionForNextInterval()
			}
			triggerActionForNextInterval()
		} catch {
			case e: InterruptedException =>
				//停止定时器
		}
	}

	/** 在指定时刻启动定时器 */
	def start(startTime: Long): Long = synchronized{
		nextTime = startTime
		thread.start()
		logInfo(s"在${nextTime}时刻，启动定时器${name}")
		nextTime
	}

	def start(): Unit = {
		start(getStartTime())
	}

	/**
		* 停止定时器，并返回上一次callback被调用的时间
		* @param interruptTimer
		* @return
		*/
	def stop(interruptTimer: Boolean):Long = synchronized{
		if (!stopped) {
			stopped = true
			if (interruptTimer) {
				thread.interrupt()
			}
			thread.join()
			logInfo(s"在${prevTime}时刻关闭定时器${name}")
		}
		prevTime
	}

	private def triggerActionForNextInterval(): Unit = {
		clock.waitTillTime(nextTime)
		callback(nextTime)
		prevTime = nextTime
		nextTime += period
		logInfo(s"在${prevTime}时定时器${name}的回调函数被调用")
	}


	/**
		* 起始时间要按照period对齐，即起始时间必须为period的整数倍且要大
		* 于当前时间。
		*/
	def getStartTime(): Long =  {
		(math.floor(clock.getTimeMillis().toDouble / period) + 1).toLong * period
	}

	def getRestartTime(originalStartTime: Long): Long = {
		(math.floor((clock.getTimeMillis() - originalStartTime).toDouble / period) + 1).toLong * period
	}

}

object RecurringTimer extends Logging {
	def main(args: Array[String]): Unit = {
		val callback = (time: Long) => {logInfo(s"echo: ${time}")}
		val timer = new RecurringTimer(new GSystemClock,50, callback,"echo")
		timer.start()
		Thread.sleep(30 * 1000)
		timer.stop(true)
	}
}