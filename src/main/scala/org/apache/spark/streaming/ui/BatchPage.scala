package org.apache.spark.streaming.ui

import javax.servlet.http.HttpServletRequest
import org.apache.spark.Logging
import org.apache.spark.ui.WebUIPage

import scala.xml.Node

class BatchPage(parent: STab) extends WebUIPage("batch") with Logging{
	private val sJobListener = parent.listener
	private val sparkListener = parent.ssc.sc.jobProgressListener

	override def render(request: HttpServletRequest): Seq[Node] = {
		logInfo("开始渲染Batch界面....")

		<html>
			<head>
				<title>Batch界面</title>
			</head>
			<body>
				<h1> </h1>
			</body>
		</html>

	}
}
