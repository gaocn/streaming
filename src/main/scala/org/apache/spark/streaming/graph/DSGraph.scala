package org.apache.spark.streaming.graph

import org.apache.spark.Logging
import org.apache.spark.streaming.{Duration, SContext}

final class DSGraph extends Serializable with Logging{


	var batchDuration: Duration = null


	// TODO
	def setContext(ssc: SContext): Unit = {
		logWarning("尚未实现该方法")
	}

	// TODO
	def restoreCheckpointData() = ???


	def setBatchDuration(batchDur: Duration):Unit = {
		this.synchronized{
			require(batchDur != null, "流处理应用必须指定Batch Duration")
			this.batchDuration = batchDur
		}
	}

}
