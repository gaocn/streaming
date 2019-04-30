package org.apache.spark.streaming

import org.apache.spark.{SparkConf, SparkContext}

class TestSContext extends STestSuiteBase {

	//HDFS测试地址：hdfs://10.230.150.174:9000/checkpoint
	test("checkpoint") {

	}


	test("create") {
		val conf = new SparkConf()
  		.setAppName("")
  		//.setMaster("local")
  		.setMaster("local[2]")
		val sc = new SparkContext(conf)
		val ssc = new SContext(sc, Durations.seconds(5))

		ssc.start()
		ssc.awaitTermination()
	}
}
