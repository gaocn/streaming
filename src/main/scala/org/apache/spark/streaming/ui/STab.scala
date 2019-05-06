package org.apache.spark.streaming.ui

import java.lang.management.ThreadMXBean

import org.apache.spark.Logging
import org.apache.spark.streaming.SContext
import org.apache.spark.streaming.ui.STab._
import org.apache.spark.ui.{SparkUI, SparkUITab}

/**
	* 通过Web UI展示流处理程序的统计信息，默认假设SparkContext启用了
	* SparkUI。
	*
	*
	* 会在WebUI中添加一个Tab：
	* 	连接为：/streaming/
	*			该Tab的主页为：/streaming/ --> SPage
	*									 /streaming/batch --> BatchPage
	*
	*/
private[spark] class STab(val ssc: SContext) extends SparkUITab(getSparkUI(ssc), "streaming") with Logging{
	logInfo("创建STab....")

	private val STATIC_RESOURCE_DIR = "org/apache/spark/streaming/ui/static"

	val parent = getSparkUI(ssc)
	val listener = ssc.progressListener
	logInfo("将Streaming Job Listener添加到JobScheduler的监听总线中已接收作业调度器产生的事件")
	ssc.addStreamingListener(listener)
	logInfo("将Streaming Job Listener添加到Spark的监听总线中以接收Spark作业运行产生的事件")
	ssc.sc.addSparkListener(listener)
	attachPage(new SPage(this))
	attachPage(new BatchPage(this))

	def detach(): Unit = {
		logInfo("将Streaming UITab从SparkUI上移除")
		getSparkUI(ssc).detachTab(this)
		getSparkUI(ssc).removeStaticHandler("/static/streaming")
	}

	def attach(): Unit = {
		logInfo("将Streaming UITab添加到SparkUI上")
		getSparkUI(ssc).attachTab(this)
		getSparkUI(ssc).addStaticHandler(STATIC_RESOURCE_DIR, "/static/streaming")
	}


	//TODO 添加TASK CPU占比，编写CPU占比算法
	//java cpu usage monitoring
	/**
		* OperatingSystemMXBean.getSystemLoadAverage(), but it
		* gives me the average system cpu load of the last minute
		*
		* You can use jMX beans to calculate a CPU load. Note
		* that this measures CPU load of your java program
		*
		* 方法1：基于Java Management API
		* 	需要添加添加监听器，在任务执行开始和结束时记录任务的CPU时间
		* 	(getCurrentThreadCpuTime)和系统时间(System.nanos())
		* 	最后通过：
		* 	(startThreadCpuTime - endThreadCpuTime)/(endSysTime - startSysTime)
		* 	计算每个任务或作业cpu使用率。
		*
		* 方法2：CPU性能评估算法
		* 开启线程每分钟对当前执行任务时记录使用的CPU CORE数与空闲的CPU
		* 核数，最后通过: (sumN(sumM(Zi)))/(N * M)
		* 其中Zi表示当前CORE是空闲还是忙用0和1表示，M表示对M分钟内的任务
		* 每分钟计算一次，N表示CORE数目。
		*
		*	实现方式：Runtime.exec确定PID，然后用PS -ef确定CPU负载
		*
		*/



}

object STab {
	def getSparkUI(ssc: SContext):SparkUI =  {
		ssc.sc.ui.getOrElse{
			throw new Exception("父SparkUI为空，无法将当前Tab附加上去!")
		}
	}


	def main(args: Array[String]): Unit = {

		import java.lang.management.ManagementFactory
		import java.lang.management.OperatingSystemMXBean

		//Java Management API 仅仅适用于类Unix系统
		val myOsBean: OperatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean
		val load = myOsBean.getSystemLoadAverage
		println(myOsBean.getAvailableProcessors)
		println(load)



		val threadMXBean: ThreadMXBean = ManagementFactory.getThreadMXBean
		println(threadMXBean.getCurrentThreadCpuTime)
		println(threadMXBean.getThreadCount)
	}
}