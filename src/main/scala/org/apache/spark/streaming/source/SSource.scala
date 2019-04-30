package org.apache.spark.streaming.source

import com.codahale.metrics.MetricRegistry
import org.apache.spark.Logging
import org.apache.spark.metrics.source.Source
import org.apache.spark.streaming.SContext

class SSource(ssc: SContext) extends Source with Logging{
	logInfo("创建SSource监控源")
	override def sourceName: String = s"${ssc.sparkContext.appName}.StreamingMetrics"

	override val metricRegistry: MetricRegistry = {
		logInfo("创建MetricRegistry")
		new MetricRegistry()
	}
}
