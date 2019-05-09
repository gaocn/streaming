package org.apache.spark.streaming.dstream

import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.streaming.{Duration, Time}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class UnionDStream[T:ClassTag](parents: Array[DStream[T]])extends DStream[T](parents.head.ssc){
	require(parents.length > 0, "不允许合并的DStream为空!")
	require(parents.map(_.ssc).distinct.size == 1,"合并的DStream必须来自相同的SContext!")
	require(parents.map(_.slideDuration).distinct.size == 1, "合并的DStream必须要有相同的slide duration!")

	/** 当前DStream实例的所有父亲DStream列表 */
	override def dependencies: List[DStream[_]] = parents.toList

	/** DStream生成RDD的时间间隔 */
	override def slideDuration: Duration = parents.head.slideDuration

	/** 在指定时刻生成RDD的方法 */
	override def compute(validTime: Time): Option[RDD[T]] = {
		val rdds = new ArrayBuffer[RDD[T]]()
		parents.map(_.getOrCompute(validTime)).foreach{
			case Some(rdd)=> rdds += rdd
			case None=>  throw new Exception("两个合并的DStream无法在" +
				s"${validTime}时刻都产生对应的RDDs！")
		}

		if (rdds.size > 0) {
			Some(new UnionRDD(ssc.sc, rdds))
		} else {
			None
		}
	}
}
