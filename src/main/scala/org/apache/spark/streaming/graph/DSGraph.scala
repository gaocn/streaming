package org.apache.spark.streaming.graph

import org.apache.spark.Logging
import org.apache.spark.streaming.{Duration, SContext}

final class DSGraph extends Serializable with Logging{
	def getInputStreams() = ???

	def getInputStreamName(streamId: Int): Option[String] = Some("test-streamid")

	def getReceiverInputStreams() = Seq.empty

	logInfo("创建DSGraph")

	def validate(): Unit = {
		logInfo("开始对DSGraph进行验证....")
	}

	var batchDuration: Duration = null

	def setContext(ssc: SContext): Unit = {
		logInfo("设置DSGraph中的SContext")
	}

	def restoreCheckpointData() = {
		logInfo("DSGraph开始从checkpoint中恢复...")
	}


	def setBatchDuration(batchDur: Duration):Unit = {
		logInfo(s"设置DSGraph中的duration为：${batchDur}")
		this.synchronized{
			require(batchDur != null, "流处理应用必须指定Batch Duration")
			this.batchDuration = batchDur
		}
	}

	def remember(duration: Duration): Unit = {
		logInfo(s"设置DSGraph中的remember duration为：${duration}")
	}
}
