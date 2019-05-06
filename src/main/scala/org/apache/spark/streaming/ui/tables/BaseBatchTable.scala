package org.apache.spark.streaming.ui.tables

import org.apache.spark.streaming.ui.BatchUIData
import org.apache.spark.ui.UIUtils
import org.apache.spark.streaming.ui.tables.BaseBatchTable.failureReasonCell

import scala.xml.Node

abstract class BaseBatchTable(tableId: String, batchInterval: Long) {
	protected def columns: Seq[Node] = {
		<th>批次时间</th>
			<th>输入大小</th>
			<th>调度延迟
				{UIUtils.tooltip("JobScheduler提交batch作业到队列所用时间", "top")}
			</th>
			<th>处理时间
				{UIUtils.tooltip("处理batch所有作业所用的时间", "top")}
			</th>
	}

	private def batchTable: Seq[Node] = {
		<table id={tableId} class="table table-bordered table-striped table-condensed sortable">
			<thead>{columns}</thead>
			<tbody>{renderRows}</tbody>
		</table>
	}

	def toSeqNode: Seq[Node] = batchTable

	/**
		* 表内容需要不同表实现
		*/
	protected def renderRows: Seq[Node]

	/**
		* 返回batch中出现的第一个出错原因
		*/
	protected def getFirstFailureReason(batchUIDatas: Seq[BatchUIData]): Option[String] = {
		batchUIDatas.flatMap(_.outputOperations.flatMap(_._2.failureReason)).headOption
	}

	protected def getFirstFailureTableCell(batchUIData: BatchUIData): Seq[Node] = {
		val firstFailureReason = batchUIData.outputOperations.flatMap(_._2.failureReason).headOption

		firstFailureReason.map { failureReason =>
			failureReasonCell(failureReason, 1, true)
		}.getOrElse(<td>-</td>)
	}

	protected def baseRow(batchUIData: BatchUIData):Seq[Node] = {
		val batchTime = batchUIData.batchTime
		val formattedBatchTime = UIUtils.formatDuration(batchTime.milliseconds)
		val eventCount = batchUIData.numTotalRecords
		val schedulingDelay = batchUIData.schedulingDelay
		val processingDelay = batchUIData.processingDelay
		val formattedSchedulingDelay = schedulingDelay.map(UIUtils.formatDuration).getOrElse("-")
		val formattedProcessingDelay = processingDelay.map(UIUtils.formatDuration).getOrElse("-")

		val batchTimeId = s"batch-${batchTime}"

		<td id={batchTimeId} sorttable_customkey={batchTime.toString} isFailed={batchUIData.isFailed.toString}>
			<a href={s"batch?id=${batchTime}"}>{formattedBatchTime}</a>
		</td>
			<td sorttable_customkey={eventCount.toString}>{eventCount.toString} events</td>
			<td sorttable_customkey={schedulingDelay.getOrElse(Long.MaxValue).toString}>{formattedSchedulingDelay}</td>
			<td sorttable_customkey={processingDelay.getOrElse(Long.MaxValue).toString}>{formattedProcessingDelay}</td>
	}

	protected def createOutputOperationProcessBar(batchUIData: BatchUIData):Seq[Node] = {
		<td class="progress-cell">
			{UIUtils.makeProgressBar(
			started = batchUIData.numActiveOutputOp,
			completed = batchUIData.numCompletedOutputOp,
			failed = batchUIData.numFailedOutputOp,
			skipped = 0,
			total = batchUIData.outputOperations.size
		)}
		</td>
	}
}

object BaseBatchTable {
	def failureReasonCell(
												 failureReason: String,
												 rowspan: Int = 1,
												 includeFirstLineInExpandDetails: Boolean = true): Seq[Node] = {

		val firstLineIndex = failureReason.indexOf("\n")
		val isMultiLine = firstLineIndex > 0
		val failureReasonSummary = if (isMultiLine) {
			failureReason.substring(0, firstLineIndex)
		} else {
			failureReason
		}

		val failureDetails = if (isMultiLine && includeFirstLineInExpandDetails) {
			failureReason.substring(firstLineIndex + 1)
		} else {
			failureReason
		}

		val failureDetailUI = if (isMultiLine) {
			<span onclick="this.parentNode.querySelector('.stacktrace-details').classList.toggle('collapsed')" class="expand-details">
			</span> ++
				<div class="stacktrace-details collapsed">
					<pre>
						{failureDetails}
					</pre>
				</div>

		} else {
			""
		}

		if (rowspan == 1) {
			<td valign="middle" style="max-width: 300px">
				{failureReasonSummary}
			</td>
		} else {
			<td valign="middle" style="max-width: 300px" rowspan={rowspan.toString}>
				{failureReasonSummary}{failureDetailUI}
			</td>
		}
	}
}