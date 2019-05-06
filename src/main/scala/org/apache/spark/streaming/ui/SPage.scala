package org.apache.spark.streaming.ui

import javax.servlet.http.HttpServletRequest
import org.apache.spark.Logging
import org.apache.spark.streaming.ui.tables.{ActiveBatchTable, CompletedBatchTable}
import org.apache.spark.ui.{UIUtils, WebUIPage}

import scala.xml.Node


class SPage(parent: STab) extends WebUIPage("") with Logging{
		private val listener = parent.listener
		private val startTime = System.currentTimeMillis()



	override def render(request: HttpServletRequest): Seq[Node] = {
		logInfo("开始渲染流处理界面....")

		val content = generateLoadResources() ++
			generateBasicInfo() ++
			listener.synchronized{
				generateBatchListTable()
			}

		UIUtils.headerSparkPage("流处理",content, parent, Some(5000))
	}


	/**
		* 生成加载JS、CSS的html标签
		*/
	private def generateLoadResources(): Seq[Node] = {
		<script src={UIUtils.prependBaseUri("/static/d3.min.js")}></script>
		<link rel="stylesheet" href={UIUtils.prependBaseUri("/static/streaming/streaming-page.css")} type="text/css"/>
		<script src={UIUtils.prependBaseUri("/static/streaming/streaming-page.js")}></script>
	}

	/**
		* 当前应用程序的batch作业统计信息
		* @return
		*/
	private def generateBasicInfo():Seq[Node] = {
		val timeSinceStart = System.currentTimeMillis() - startTime

		<div>当前正在为batchDuration为
			<strong>
				{UIUtils.formatDuration(listener.batchDuration)}
			</strong>
			运行了
			<strong>
				{UIUtils.formatDurationVerbose(timeSinceStart)}
			</strong>
			，开始时间为
			<strong>
				{UIUtils.formatDate(startTime)}
			</strong>
			(已完成batches：<strong>{listener.numTotalCompletedBatches}</strong>，已接收记录数：<strong>{listener.numTotalReceivedRecords}</strong>)
		</div>
		<br/>
	}


	/**
		* 每次batch的详细信息
		*/
	private def generateBatchListTable():Seq[Node] = {

		val runningBatches:Seq[BatchUIData] =  listener.runningBatches.map(b => BatchUIData(b)).sortBy(_.batchTime.milliseconds).reverse
		val waitingBatches: Seq[BatchUIData] = listener.waitingBatches.map(b => BatchUIData(b)).sortBy(_.batchTime.milliseconds).reverse
		val completedBatches: Seq[BatchUIData] =  listener.retainedCompletedBatches.map(b => BatchUIData(b)).sortBy(_.batchTime.milliseconds).reverse

		val activeBatchesContent =
			<h4 id="active">
				正在进行的批次({runningBatches.size + waitingBatches.size})
			</h4> ++
		new ActiveBatchTable(runningBatches, waitingBatches, listener.batchDuration).toSeqNode

		val completedBatchesContent =
			<h4 id="completed">
				已完成批次({completedBatches.size}/{listener.numTotalCompletedBatches})
			</h4> ++
		new CompletedBatchTable(completedBatches, listener.batchDuration).toSeqNode

		activeBatchesContent ++ completedBatchesContent
	}

}

object SPage {
	val BLACK_RIGHT_TRIANGLE_HTML = "&#9654;"
	val BLACK_DOWN_TRIANGLE_HTML = "&#9660;"
	val emptyCell = "-"

	/**
		* 将时间格式为可读的，例如：5 seconds 34ms，若无时间返回"-"
		*/
	def formatDurationOption(msOption: Option[Long]): String = {
		msOption.map(UIUtils.formatDurationVerbose).getOrElse(emptyCell)
	}
}
