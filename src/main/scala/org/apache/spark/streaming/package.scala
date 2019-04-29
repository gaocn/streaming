package org.apache.spark

/**
	* 1、StreamingContext为Spark Streaming应用程序的入口点；
	* 2、DStream代表源源不断的一些RDD集合，代表的数据流；
	* 3、PairDStreamFunctions中的函数用于所有Key-Value的DStream中，
	* 通过隐式转换自动被引入使用，例如：groupByKey、reduceByKey算子；
	* 4、对Java语言提供了JavaStreamingContext、JavaDStream、
	* JavaPairDStream；
	*/
package object streaming {
	//for package doc only
}
