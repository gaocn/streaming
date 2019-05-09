package org.apache.spark.streaming.dstream

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, Time}

import scala.reflect.ClassTag

class FilteredDStream[T:ClassTag](parent:DStream[T], filterFunc:T=>Boolean)extends DStream[T](parent.ssc){
	/** 在指定时刻生成RDD的方法 */
	override def compute(validTime: Time): Option[RDD[T]] = {
		parent.getOrCompute(validTime).map(_.filter(filterFunc))
	}

	/** 当前DStream实例的所有父亲DStream列表 */
	override def dependencies: List[DStream[_]] = List(parent)

	/** DStream生成RDD的时间间隔 */
	override def slideDuration: Duration = parent.slideDuration
}
