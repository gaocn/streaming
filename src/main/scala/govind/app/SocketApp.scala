package govind.app

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Durations, SContext}
import org.apache.spark.streaming.receiver.impl.SocketReceiver

object SocketApp {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("SocketInput")
			.setMaster("local[2]")
		val sc = new SparkContext(conf)
		val ssc = new SContext(sc, Durations.seconds(5))

		val stream = ssc.socketStream("localhost",8080, SocketReceiver.bytesToLines, StorageLevel.MEMORY_ONLY)

		stream.print()

		ssc.start()
		ssc.awaitTermination()
	}
}
