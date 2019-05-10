package org.apache.spark.streaming.rate

import org.apache.spark.{Logging, SparkConf}
import com.google.common.util.concurrent.{RateLimiter=>GuavaRateLimiter}
/**
	* 提供BackPressure反压机制，实现对Receiver消费数据的速度。
	*
	* 当太多消息被推送过来太快时，{@code waitToPush}方法会阻塞线程，并
	* 等待只有一条新的消息被push(假定消息一次push过来一条)过来后才会返回。
	*
	* 通过配置spark.streaming.receiver.maxRate可以控制Receiver每秒
	* 接收数据的速率。
	*
	* 实现类：
	* 	BlockGenerator限制数据存储速度进而限定输入流入速度。
	*
	*/
private[streaming] abstract class RateLimiter(conf:SparkConf) extends Logging{
	logInfo(s"${this.getClass.getName}限流器初始化")

	private val maxRateLimit = conf.getLong("spark.streaming.receiver.maxRate", Long.MaxValue)
	private lazy val rateLimiter = GuavaRateLimiter.create(maxRateLimit.toDouble)

	/**
		* 超过限流速度，则阻塞当前线程，直到有可用资源。
		*/
	def waitToPush(): Unit = {
		rateLimiter.acquire()
	}

	/**
		* 返回当前的限流速度，若没有设置限流返回{{Long.MaxValue}}
		*/
	def getCurrentLimit:Long = rateLimiter.getRate.toLong

	/**
		* 设置限流速度为`newRate`，不能超过{{Long.MaxValue}}
		* @param newRate 0或负数无效，新的限流速度
		*/
	def updateRate(newRate: Int):Unit =  {
		if(newRate > 0) {
			if(maxRateLimit > 0) {
				rateLimiter.setRate(newRate.min(maxRateLimit))
			} else {
				rateLimiter.setRate(newRate)
			}
		}
	}
}
