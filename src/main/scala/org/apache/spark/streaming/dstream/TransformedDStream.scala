package org.apache.spark.streaming.dstream

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, Time}

import scala.reflect.ClassTag

class TransformedDStream[U:ClassTag](parents: Seq[DStream[_]], realTransformFunc: (Seq[RDD[_]], Time) => RDD[U]) extends DStream[U](parents.head.ssc) {

	/** 当前DStream实例的所有父亲DStream列表 */
	override def dependencies: List[DStream[_]] = parents.toList

	/** DStream生成RDD的时间间隔 */
	override def slideDuration: Duration = parents.head.slideDuration

	/** 在指定时刻生成RDD的方法 */
	override def compute(validTime: Time): Option[RDD[U]] = {
		val parentRDDs = parents.map(_.getOrCompute(validTime).getOrElse(throw new Exception(s"无法从父亲DStream中获取${validTime}时刻的RDD")))

		/**
			* 调用compute时transformFunc方法会被执行，不是lazy级别的！
			*
			* 当在transformFunc中有RDD的Action级别操作时，它会绕过Spark
			* Streaming直接在Spark Cluster中产生物理作业！！
			*
			* 这里产生的作业不受Spark Streaming统一调度而是由Spark Core
			* 直接调度，因为transform一般是为使用RDD的内置的丰富
			* transformation操作，返回的是RDD，当在返回RDD之前是可以调用
			* RDD的Action级别的操作启动物理作业完成自己需要的功能的。
			*
			* 例如：可以用transform动态调优-> 数据倾斜问题，动态查看分区
			* 中的记录数，针对数据倾斜可以采取一定措施。
			*/
		val transformedRDD = realTransformFunc(parentRDDs, validTime)
		if(transformedRDD == null) {
			throw new Exception("transform函数不能返回空，可以返回SparkContext" +
				".emptyRDD()作为结果代表无任何元素")
		}
		Some(transformedRDD)
	}
}
