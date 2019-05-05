package org.apache.spark.streaming.ui

import org.apache.spark.Logging
import org.apache.spark.streaming.SContext
import org.apache.spark.ui.{SparkUI, SparkUITab}
import STab._

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
private[spark] class STab(ssc: SContext) extends SparkUITab(getSparkUI(ssc), "streaming") with Logging{
	logInfo("创建STab....")

	private val STATIC_RESOURCE_DIR = "org/apache/spark/streaming/ui"

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
}

object STab {
	def getSparkUI(ssc: SContext):SparkUI =  {
		ssc.sc.ui.getOrElse{
			throw new Exception("父SparkUI为空，无法将当前Tab附加上去!")
		}
	}
}