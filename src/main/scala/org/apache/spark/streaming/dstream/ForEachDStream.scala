package org.apache.spark.streaming.dstream

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.scheduler.Job
import org.apache.spark.streaming.{Duration, Time}

import scala.reflect.ClassTag

/**
	*
	*/
class ForEachDStream[T:ClassTag](parent: DStream[T], foreachFunc: (RDD[T], Time) => Unit, displayInnerRDDOps: Boolean)extends DStream[Unit](parent.ssc){

	/** 在指定时刻生成RDD的方法 */
	override def compute(validTime: Time): Option[RDD[Unit]] =None

	/** 当前DStream实例的所有父亲DStream列表 */
	override def dependencies: List[DStream[_]] =  List(parent)

	/** DStream生成RDD的时间间隔 */
	override def slideDuration: Duration = parent.slideDuration

	override def generateJob(time: Time): Option[Job] = {
		parent.getOrCompute(time) match {
			case Some(rdd) =>
				val jobFunc = ()=>createRDDWithLocalProperties(time, displayInnerRDDOps){
					foreachFunc(rdd, time)
				}
				Some(new Job(time, jobFunc))
			case None => None
		}
	}

}
