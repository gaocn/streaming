package org.apache.spark.streaming.ui

import javax.servlet.http.HttpServletRequest
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.spark.Logging
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.ui.SJobProgressListener.SparkJobId
import org.apache.spark.streaming.ui.tables.BaseBatchTable
import org.apache.spark.streaming.util.DateTimeUtilis
import org.apache.spark.ui.jobs.UIData.JobUIData
import org.apache.spark.ui.{UIUtils, WebUIPage}

import scala.xml.{Node, NodeSeq, Unparsed}

case class SparkJobIdWithUIData(sparkJobId:SparkJobId,jobUIData: Option[JobUIData])

private[streaming] class BatchPage(parent: STab) extends WebUIPage("batch") with Logging{
	private val sJobListener = parent.listener
	private val sparkListener = parent.ssc.sc.jobProgressListener

	override def render(request: HttpServletRequest): Seq[Node] = {
		logInfo("开始渲染Batch界面....")
		val batchTime  = Option(request.getParameter("id")).map(id=>Time(id.toLong)).getOrElse{
			logInfo("缺少id查询参数")
			throw new IllegalArgumentException("缺少id查询参数")
		}

		val formattedBatchTime = DateTimeUtilis.formatBatchTime(batchTime.milliseconds,
			sJobListener.batchDuration)
		val batchUIData  = sJobListener.getBatchUIData(batchTime).getOrElse{
			logInfo(s"${batchTime}批次的信息统计不存在")
			throw new IllegalArgumentException(s"${batchTime}批次的信息统计不存在")
		}


		val formattedSchedulingDelay = batchUIData.schedulingDelay.map(UIUtils.formatDuration).getOrElse("-")
		val formattedProcessingDelay = batchUIData.processingDelay.map(UIUtils.formatDuration).getOrElse("-")
		val formattedTotalDelay = batchUIData.totalDelay.map(UIUtils.formatDuration).getOrElse("-")

		val inputMetadata = batchUIData.streamIdToInputInfo.values.flatMap{inputInfo =>
			inputInfo.metadataDescription.map(desc=>inputInfo.inputStreamId -> desc)
		}.toSeq

		val summary: NodeSeq  = <div>

			<ul  class="unstyled">
				<li>
					<strong>Batch Duration：</strong>
					{UIUtils.formatDuration(sJobListener.batchDuration)}
				</li>

				<li>
					<strong>输入数据大小：</strong>
					{batchUIData.numTotalRecords} 条
				</li>

				<li>
					<strong>调度延迟：</strong>
					{formattedSchedulingDelay}
				</li>

				<li>
					<strong>处理延迟：</strong>
					{formattedProcessingDelay}
				</li>


				<li>
					<strong>总延迟：</strong>
					{formattedTotalDelay}
				</li>

				{
					if(inputMetadata.nonEmpty) {
						<li>
							<strong>输入元数据：</strong>
							{generateInputMetadataTable(inputMetadata)}
						</li>
					}
				}
			</ul>
		</div>

		val content = summary ++ generateJobTable(batchUIData)
		UIUtils.headerSparkPage("", content, parent)
	}

	private def columns: Seq[Node] =  {
		<th>输出操作Id</th>
		<th>描述</th>
		<th>状态</th>
		<th>作业Id</th>
		<th>作业运行时间(从提交到队列到执行完毕)</th>
		<th class="sorttable_nosort">阶段: 成功数/总数</th>
		<th class="sorttable_nosort">所有阶段的任务: 成功数/总数</th>
		<th>Error</th>
	}

	private def generateOutputOpRowWithoutSparkJobs(outputOpData: OutputOperationUIData, outputOpDesc: Seq[Node], formattedOutputOpDuration: String): Seq[Node] =  {
		<tr>
			<td class="output-op-id-cell">{outputOpData.id.toString}</td>
			<td>{outputOpDesc}</td>
			<td>{formattedOutputOpDuration}</td>
			<!-- 作业Id -->
			<td>-</td>
			<!-- 作业运行时间 -->
			<td>-</td>
			<!-- 阶段: 成功数/总数 -->
			<td>-</td>
			<!-- 所有阶段的任务: 成功数/总数 -->
			<td>-</td>
		</tr>
	}

	private def generateJobRow(outputOpData: OutputOperationUIData,outputOpDescription: Seq[Node],formattedOutputOpDuration: String,numSparkJobRowsInOutputOp: Int,isFirstRow: Boolean, sparkJob: SparkJobIdWithUIData): Seq[Node] = {
		if (sparkJob.jobUIData.isDefined) {
			generateNormalJobRow(outputOpData, outputOpDescription, formattedOutputOpDuration,
				numSparkJobRowsInOutputOp, isFirstRow, sparkJob.jobUIData.get)
		} else {
			generateDroppedJobRow(outputOpData, outputOpDescription, formattedOutputOpDuration,
				numSparkJobRowsInOutputOp, isFirstRow, sparkJob.sparkJobId)
		}
	}

	/**
		* 为显示Spark作业信息创建一行，同时需要将output op信息折叠显示
		*/
	private def generateNormalJobRow(outputOperationUIData: OutputOperationUIData, outputOpDesc:Seq[Node], formattedOutputOpDuration:String, numSparkJobRowsInOutputOp:Int,isFirstRow:Boolean, sparkJob: JobUIData): Seq[Node] = {
		val duration: Option[Long] = {
			sparkJob.submissionTime.map{start =>
				val end = sparkJob.completionTime.getOrElse(System.currentTimeMillis())
				end - start
			}
		}

		//获取最近一次发生错误时的错误信息
		val lastFailureReason = sparkJob.stageIds.sorted.reverse
			.flatMap(sparkListener.stageIdToInfo.get)
			.dropWhile(_.failureReason == None).take(1)
  		.flatMap(info => info.failureReason).headOption.getOrElse("")
		val formattedDuration = duration.map(UIUtils.formatDuration).getOrElse("-")
		val detailUrl = s"${UIUtils.prependBaseUri(parent.basePath)}/jobs/job?id=${sparkJob.jobId}"

		//第一行显示output op的id和信息，其他行需要折叠起来
		val prefixCells = if(isFirstRow) {
			<td class="output-op-id-cell" rowspan={numSparkJobRowsInOutputOp.toString}>{outputOperationUIData.id.toString}</td>
			<td rowspan={numSparkJobRowsInOutputOp.toString}>{outputOpDesc}</td>
			<td rowspan={numSparkJobRowsInOutputOp.toString}>{outputOpStatusCell(outputOperationUIData, numSparkJobRowsInOutputOp)}</td>
		} else {
			Nil
		}

		<tr>
			{prefixCells}
			<td sortable_customkey={sparkJob.jobId.toString}>
				<a href={detailUrl}>
					{sparkJob.jobId}{sparkJob.jobGroup.map(id=>s"(${id})").getOrElse("")}
				</a>
			</td>

			<td sortable_customkey={duration.getOrElse(Long.MaxValue).toString}>
				{formattedDuration}
			</td>

			<td class="stage-progress-cell">
				{sparkJob.completedStageIndices.size}/{sparkJob.stageIds.size - sparkJob.numSkippedStages}
				{if(sparkJob.numFailedStages > 0) s"${sparkJob.numFailedStages}失败" }
				{if(sparkJob.numSkippedStages > 0) s"${sparkJob.numSkippedStages}跳过" }
			</td>

			<td class="progress-cell">
				{
					UIUtils.makeProgressBar(
						started = sparkJob.numActiveTasks,
						completed = sparkJob.numCompletedTasks,
						failed = sparkJob.numFailedTasks,
						skipped = sparkJob.numSkippedTasks,
						total = sparkJob.numTasks - sparkJob.numSkippedTasks
					)
				}
			</td>
			{BaseBatchTable.failureReasonCell(lastFailureReason)}
		</tr>


	}

	private def outputOpStatusCell(outputOP:OutputOperationUIData, rowspan:Int):Seq[Node] = {
		outputOP.failureReason match {
			case Some(failureReason) =>
				BaseBatchTable.failureReasonCell(failureReason,rowspan, false)
			case None =>
				if(outputOP.endTime.isEmpty) {
					<td rowspan={rowspan.toString}>-</td>
				} else {
					<td rowspan={rowspan.toString}>成功</td>
				}
		}
	}

	/**
		* 若由于超过限制，某个作业的信息被SparkListener丢弃，这里只显示
		* 作业id，其他均用"-"代替。
		*/
	private def generateDroppedJobRow(outputOperationUIData: OutputOperationUIData, outputOpDesc:Seq[Node], formattedOutputOpDuration:String, numSparkJobRowsInOutputOp:Int,isFirstRow:Boolean,jobId:Int):Seq[Node] = {
		//第一行显示output op的id和信息，其他行需要折叠起来
		val prefixCells = if(isFirstRow) {
			<td class="output-op-id-cell" rowspan={numSparkJobRowsInOutputOp.toString}>{outputOperationUIData.id.toString}</td>
				<td rowspan={numSparkJobRowsInOutputOp.toString}>{outputOpDesc}</td>
				<td rowspan={numSparkJobRowsInOutputOp.toString}>{outputOpStatusCell(outputOperationUIData, numSparkJobRowsInOutputOp)}</td>
		} else {
			Nil
		}

		<tr>
			{prefixCells}
			<td sorttable_customkey={jobId.toString}>
				{if (jobId >= 0) jobId.toString else "-"}
			</td>
			<!-- 作业运行时间 -->
			<td>-</td>
			<!-- 阶段: 成功数/总数 -->
			<td>-</td>
			<!-- 所有阶段的任务: 成功数/总数 -->
			<td>-</td>
			<!-- 错误信息 -->
			<td>-</td>
		</tr>
	}


	private def generateOutputOpIdRow(outputOperationUIData: OutputOperationUIData, sparkJobs:Seq[SparkJobIdWithUIData]):Seq[Node] =  {
		val formattedOutputOpDuration = outputOperationUIData.duration.map(UIUtils.formatDuration).getOrElse("-")

		val descriptionUI = {
			<div>
				{outputOperationUIData.name}
				<span onclick="this.parentNode.qureySelector('.stage-details').classList.toggle('collapsed')" class="expand-details">
					+ details
				</span>
				<div class="stage-details collapsed">
					<pre>{outputOperationUIData.description}</pre>
				</div>
			</div>
		}

		if(sparkJobs.isEmpty) {
			generateOutputOpRowWithoutSparkJobs(outputOperationUIData, descriptionUI, formattedOutputOpDuration)
		} else {
			val firstRow = generateJobRow(outputOperationUIData, descriptionUI, formattedOutputOpDuration, sparkJobs.size, true, sparkJobs.head)

			val tailRows = sparkJobs.tail.map{sparkJob =>
				generateJobRow(outputOperationUIData, descriptionUI, formattedOutputOpDuration,sparkJobs.size, false, sparkJob)
			}
			(firstRow ++ tailRows).flatten
		}
	}

	private def getJobData(sparkJobId: SparkJobId):Option[JobUIData] =  {
		sparkListener.activeJobs.get(sparkJobId).orElse{
			sparkListener.completedJobs.find(_.jobId == sparkJobId).orElse{
				sparkListener.failedJobs.find(_.jobId == sparkJobId)
			}
		}
	}


	private def generateJobTable(batchUIData: BatchUIData):Seq[Node] = {
		val outputIdToSparkJobIds =batchUIData.outputOpIdSparkJobIdPairs
			.groupBy(_.outputOpID)
  		.map{case(outputId, outputOpIdAndSparkJobIds) =>
				(outputId, outputOpIdAndSparkJobIds.map(_.sparkJobId).sorted)
			}

		val outputOps: Seq[(OutputOperationUIData, Seq[SparkJobId])] = batchUIData.outputOperations
  		.map{case (outputOpId, outputOpUIData)  =>
				val sparkJobids = outputIdToSparkJobIds.get(outputOpId).getOrElse(Seq.empty)
				(outputOpUIData, sparkJobids)
			}.toSeq.sortBy(_._1.id)

		sparkListener.synchronized{
			val outputOpWithJobs =  outputOps.map{case (outputOpIOData, sparkJobIds) =>
				(outputOpIOData, sparkJobIds.map(sparkJobId=>SparkJobIdWithUIData(sparkJobId, getJobData(sparkJobId))))
			}

			<table id="batch-job-table" class="table table-borded table-striped table-condensed">
				<thead>{columns}</thead>
				<tbody>
					{
						outputOpWithJobs.map{case (outputOpUIData, sparkJobIdsWithUIData) =>
							generateOutputOpIdRow(outputOpUIData,sparkJobIdsWithUIData)
						}
					}
				</tbody>
			</table>
		}
	}


	private def generateInputMetadataTable(inputMetadata: Seq[(Int, String)]):Seq[Node] = {
		<table class={UIUtils.TABLE_CLASS_STRIPED_SORTABLE}>
			<thead>
				<tr>
					<td>输入</td>
					<td>元数据</td>
				</tr>
			</thead>

			<tbody>
				{inputMetadata.flatMap(generateInputMetadataRow)}
			</tbody>

		</table>
	}
	def generateInputMetadataRow(inputMetadata: (Int, String)):Seq[Node] = {
		val streamId = inputMetadata._1
		<tr>
				<td>{sJobListener.streamName(streamId).getOrElse(s"Stream-${streamId}")}</td>
				<td>{
						//将Tab转换为4个空格，同时\n变为<br/>
						Unparsed(StringEscapeUtils.escapeHtml4(inputMetadata._2)
						.replaceAll("\n", "<br/>")
						.replaceAll("\t", "&nbsp;&nbsp;&nbsp;&nbsp;"))
					}</td>
		</tr>
	}
}
