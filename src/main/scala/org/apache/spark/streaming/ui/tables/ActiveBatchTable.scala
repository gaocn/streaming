package org.apache.spark.streaming.ui.tables

import org.apache.spark.streaming.ui.BatchUIData

import scala.xml.Node

class ActiveBatchTable(runningBatches: Seq[BatchUIData], waitingBatches: Seq[BatchUIData], batchInterval: Long) extends BaseBatchTable("active-batches-table", batchInterval) {
	private val firstFailureReason = getFirstFailureReason(runningBatches)

	override protected def columns: Seq[Node] = super.columns ++ {
		<th>输出Ops: 成功数/总数</th>
		<th>状态</th> ++ {
			if(firstFailureReason.nonEmpty) {
				<th>错误</th>
			} else {
				Nil
			}
		}
	}

	override protected def renderRows: Seq[Node] = {
		//`waitingBatches`的`batchTime`会比`runningBatches`的值大，
		// 因此首先展示`waitingBatches`
		waitingBatches.flatMap(batch => <tr>{waitingBatchRow(batch)}</tr>) ++
			runningBatches.flatMap(batch => <tr>{runningBatchRow(batch)}</tr>)
	}

	private def runningBatchRow(batchUIData: BatchUIData):Seq[Node] = {
		baseRow(batchUIData)  ++
			createOutputOperationProcessBar(batchUIData) ++
			<td>处理中</td> ++ {
			if(firstFailureReason.nonEmpty) {
				getFirstFailureTableCell(batchUIData)
			}  else {
				Nil
			}
		}
	}

	private def waitingBatchRow(batchUIData: BatchUIData):Seq[Node] = {
		baseRow(batchUIData)  ++
		createOutputOperationProcessBar(batchUIData) ++
		<th>排队中</th> ++ {
			//尚未运行，不可能有错误信息
			if (firstFailureReason.nonEmpty) {
				<td>-</td>
			} else {
				Nil
			}
		}
	}

}
