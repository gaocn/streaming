package org.apache.spark.streaming.ui.tables

import org.apache.spark.streaming.ui.BatchUIData
import org.apache.spark.ui.UIUtils

import scala.xml.Node

class CompletedBatchTable(batchUIData: Seq[BatchUIData], batchInterval:Long) extends BaseBatchTable("completed-batches-table",batchInterval){
	private val firstFailureReason = getFirstFailureReason(batchUIData)

	override protected def columns: Seq[Node] = super.columns ++ {
		<th>总延迟{UIUtils.tooltip("处理一个批次总耗时", "top")}</th>
		<th>输出Ops: 成功数/总数</th> ++ {
			if(firstFailureReason.nonEmpty) {
				<th>Error</th>
			} else {
				Nil
			}
		}
	}

	override protected def renderRows: Seq[Node] = {
		 batchUIData.flatMap(batchUIData => <tr>{completedBatchRow(batchUIData)}</tr>)
	}

	private def completedBatchRow(batchUIData: BatchUIData):Seq[Node] = {
		val totalDelay = batchUIData.totalDelay
		val formattedTotalDelay = totalDelay.map(UIUtils.formatDuration).getOrElse("-")

		baseRow(batchUIData) ++ {
			<td sorttable_customkey={totalDelay.getOrElse(Long.MaxValue).toString}>{formattedTotalDelay}</td>
		} ++ createOutputOperationProcessBar(batchUIData) ++ {
			if(firstFailureReason.nonEmpty) {
				getFirstFailureTableCell(batchUIData)
			} else {
				Nil
			}
		}
	}
}
