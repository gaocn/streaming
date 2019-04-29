package org.apache.spark.streaming

import org.apache.spark.{Logging, SparkConf, SparkFunSuite}
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.time.{Span, Seconds => ScalaTestSeconds}
import org.scalatest.concurrent.Eventually.timeout

trait STestSuiteBase extends SparkFunSuite with BeforeAndAfter with Logging{
	//测试类名
	def framework: String = this.getClass.getSimpleName
	//测试模式
	def master: String =  "local[2]"
	//窗口大小
	def batchDuration: Duration = Seconds(1)

	lazy val checkpointDir: String = {
		val dir = "E:\\checkpoint"
		logDebug(s"checkpoint目录为：${dir}")
		dir.toString
	}
	//测试输入分区数
	def numInputPartitions: Int = 2
	//测试超时时间
	def maxWaitTimeMillis: Int = 10000
	//时候使用manual时钟
	def useManualClock:Boolean = true
	//在修改manual时钟前，时候需要等待
	def actuallyWait: Boolean = false

	val conf = new SparkConf()
		.setAppName(framework)
  	.setMaster(master)
	//在用`eventually`测试时的超时时间
	val eventuallyTimeout: PatienceConfiguration.Timeout = timeout(Span(10, ScalaTestSeconds))

	/**
		* 根据配置决定使用系统时钟还是手动时钟
		*/
	def beforeFunction(): Unit = {
		if (useManualClock) {
			logInfo("使用manual clock时钟")
			conf.set("spark.streaming.clock", "org.apache.spark.util.GManualClock")
		} else {
			logInfo("使用real clock时钟")
			conf.set("spark.streaming.clock", "org.apache.spark.util.GSystemClock")
		}
	}

	/**
		*
		*/
	def afterFunction(): Unit = {
		System.clearProperty("spark.streaming.clock")
	}
	before(beforeFunction)
	after(afterFunction)


}
