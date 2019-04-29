package org.apache.spark.streaming.source

import com.codahale.metrics.MetricRegistry
import org.apache.spark.metrics.source.Source
import org.apache.spark.streaming.SContext

class SSource(scontext: SContext) extends Source{
	override def sourceName: String = "Streaming Source"

	override def metricRegistry: MetricRegistry = {
		throw new Exception("尚未实现")
	}
}
