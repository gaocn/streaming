package org.apache.spark.streaming.dstream

import org.apache.spark.rdd.{PartitionerAwareUnionRDD, RDD, UnionRDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, Interval, Time}

import scala.reflect.ClassTag

class WindowDStream[T:ClassTag](parent: DStream[T], _windowDuration: Duration, _slideDuration: Duration)extends DStream[T](parent.ssc){
	require(_windowDuration.isMultipleOf(parent.slideDuration), s"窗口大小(${_windowDuration})必须是父亲DStream的slideDuration(${parent.slideDuration})的整数倍")
	require(_slideDuration.isMultipleOf(parent.slideDuration), s"滑动窗口大小(${_slideDuration})必须是父亲DStream的slideDuration(${parent.slideDuration})的整数倍")

	//由于父亲DStream中的RDD需要重用，因此显示将其持久化
	parent.persist(StorageLevel.MEMORY_ONLY_SER)

	/** 当前DStream实例的所有父亲DStream列表 */
	override def dependencies: List[DStream[_]] = List(parent)

	/** DStream生成RDD的时间间隔 */
	override def slideDuration: Duration = _slideDuration

	def windowDuration:Duration = _windowDuration

	override def parentRememberDuration: Duration = rememberDuration + windowDuration

	override def persist(level: StorageLevel): DStream[T] = {
		//不要持久化当前DStream中的RDD，因为会导致多个相同的RDDs被持久化，
		// 而是通过将父亲DStream的RDD持久化以实现RDDs在滑动窗口时共享。
		parent.persist(level)
		this
	}

	/** 在指定时刻生成RDD的方法 */
	override def compute(validTime: Time): Option[RDD[T]] = {
		val currentWindow = new Interval(validTime - windowDuration + parent.slideDuration, validTime)
		val rddsInWindow = parent.slice(currentWindow)
		val windowRDD = if(rddsInWindow.flatten(_.partitioner).distinct.length == 1) {
			logInfo("使用PartitionerAwareUnionRDD将窗口中的RDDs进行合并！")
			new PartitionerAwareUnionRDD(ssc.sc, rddsInWindow)
		} else {
			logInfo("使用UnionRDD将窗口中的多个RDDs进行合并！")
			new UnionRDD(ssc.sc, rddsInWindow)
		}
		Some(windowRDD)
	}

}
