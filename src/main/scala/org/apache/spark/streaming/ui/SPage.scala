package org.apache.spark.streaming.ui

import javax.servlet.http.HttpServletRequest
import org.apache.spark.Logging
import org.apache.spark.ui.WebUIPage

import scala.xml.Node


class SPage(parent: STab) extends WebUIPage("") with Logging{
		private val listener = parent.listener
		private val startTime = System.currentTimeMillis()



	override def render(request: HttpServletRequest): Seq[Node] = {
		logInfo("开始渲染流处理界面....")


		<html>
			<head>
				<title>流处理</title>
			</head>
			<body>
				<h1>流处理页面</h1>
			</body>
		</html>
	}
}
