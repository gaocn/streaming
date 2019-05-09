package org.apache.spark.streaming.dstream

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, Time}

import scala.reflect.ClassTag

class GlommedDStream[T:ClassTag](parent:DStream[T]) extends DStream[Array[T]](parent.ssc){
	/** 在指定时刻生成RDD的方法 */
	override def compute(validTime: Time): Option[RDD[Array[T]]] = {
		parent.getOrCompute(validTime).map(_.glom())
	}

	/** 当前DStream实例的所有父亲DStream列表 */
	override def dependencies: List[DStream[_]] = List(parent)

	/** DStream生成RDD的时间间隔 */
	override def slideDuration: Duration = parent.slideDuration
}
