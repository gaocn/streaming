package org.apache.spark.streaming.dstream

import org.apache.spark.util.CallSite

import scala.util.matching.Regex

class DStream {

}

object DStream {
	/** Get the creation site of a DStream from the stack trace of when the DStream is created. */
	private[streaming] def getCreationSite(): CallSite = {
		val SPARK_CLASS_REGEX = """^org\.apache\.spark""".r
		val SPARK_STREAMING_TESTCLASS_REGEX = """^org\.apache\.spark\.streaming\.test""".r
		val SPARK_EXAMPLES_CLASS_REGEX = """^org\.apache\.spark\.examples""".r
		val SCALA_CLASS_REGEX = """^scala""".r

		/** Filtering function that excludes non-user classes for a streaming application */
		def streamingExclustionFunction(className: String): Boolean = {
			def doesMatch(r: Regex): Boolean = r.findFirstIn(className).isDefined
			val isSparkClass = doesMatch(SPARK_CLASS_REGEX)
			val isSparkExampleClass = doesMatch(SPARK_EXAMPLES_CLASS_REGEX)
			val isSparkStreamingTestClass = doesMatch(SPARK_STREAMING_TESTCLASS_REGEX)
			val isScalaClass = doesMatch(SCALA_CLASS_REGEX)

			// If the class is a spark example class or a streaming test class then it is considered
			// as a streaming application class and don't exclude. Otherwise, exclude any
			// non-Spark and non-Scala class, as the rest would streaming application classes.
			(isSparkClass || isScalaClass) && !isSparkExampleClass && !isSparkStreamingTestClass
		}
		org.apache.spark.util.Utils.getCallSite(streamingExclustionFunction)
	}
}
