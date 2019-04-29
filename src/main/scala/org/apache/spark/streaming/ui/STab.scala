package org.apache.spark.streaming.ui

import org.apache.spark.Logging
import org.apache.spark.streaming.SContext
import org.apache.spark.ui.{SparkUI, SparkUITab}
import STab._

/**
	* 通过Web UI展示流处理程序的统计信息，默认假设SparkContext启用了
	* SparkUI。
	*/
private[spark] class STab(ssc: SContext) extends SparkUITab(getSparkUI(ssc), "流处理") with Logging{


	//TODO 添加TASK CPU占比，编写CPU占比算法

}

object STab {
	def getSparkUI(ssc: SContext):SparkUI =  {
		ssc.sc.ui.getOrElse{
			throw new Exception("父SparkUI为空，无法将当前Tab附加上去!")
		}
	}
}