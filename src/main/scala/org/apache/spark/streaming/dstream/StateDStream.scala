package org.apache.spark.streaming.dstream

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, Time}

import scala.reflect.ClassTag

class StateDStream[K:ClassTag, V:ClassTag, S:ClassTag](
			parent: DStream[(K, V)],
			updateFunc: (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)],
			partitioner:  Partitioner,
			preservePartitioning: Boolean,
			initialRDD: Option[RDD[(K, S)]]
		) extends DStream[(K, S)](parent.ssc) {

	//内存+序列化形式保存数据，因为需要不断进行update计算，数据会很多，
	// 因此在每次进行updateStateByKey时都要将数据持久化。
	super.persist(StorageLevel.MEMORY_ONLY_SER)


	/** 当前DStream实例的所有父亲DStream列表 */
	override def dependencies: List[DStream[_]] = List(parent)

	/** DStream生成RDD的时间间隔 */
	override def slideDuration: Duration = parent.slideDuration

	override val mustCheckpoint = true

	/** 在指定时刻生成RDD的方法 */
	override def compute(validTime: Time): Option[RDD[(K, S)]] = {
		getOrCompute(validTime - slideDuration) match {
			//1.1 previous state RDD exists
			case Some(prevStateRDD) => {
				//2. try to get parent rdd
				parent.getOrCompute(validTime) match {
					case Some(parentRDD) =>
						//3.1 根据cogroup直接计算结果
						computeUsingPrevisousRDD(parentRDD, prevStateRDD)
					case None =>
						//3.2 若parent rdd不存在，则仅仅将更新操作作用于prevStateRDD上
						val updateFuncLocal = updateFunc
						val finalFunc  = (iter: Iterator[(K, S)]) => {
							val i = iter.map(t => (t._1, Seq[V](), Option(t._2)))
							updateFuncLocal(i)
						}
						val stateRDD = prevStateRDD.mapPartitions(finalFunc, preservePartitioning)
						Some(stateRDD)
				}
			}

			//1.2 之前没有StateRDD，说明该RDD为第一个输入RDD
			case None => {
				//2. try to get parent rdd
				parent.getOrCompute(validTime) match {
					case Some(parentRDD) =>
						//3.1 将intialRDD作为prevStateRDD与parentRDD进行更新
						initialRDD match {
							case Some(initialStateRDD) =>
								computeUsingPrevisousRDD(parentRDD, initialStateRDD)
							case None =>
								//4 若intialRDD为空，则仅仅将更新函数作用于parentRDD
								val updateFuncLocal = updateFunc
								val finalFunc = (iter: Iterator[(K, Iterable[V])])  => {
									val i = iter.map(t => (t._1, t._2.toSeq, None))
									updateFuncLocal(i)
								}

								val groupedRDD = parentRDD.groupByKey(partitioner)
								val sessionRDD = groupedRDD.mapPartitions(finalFunc, preservePartitioning)
								Some(sessionRDD)
						}
					case None =>
						//3.2 没有数据，不需要操作
						None
				}
			}
		}
	}

	/**
		* updateStateByKey操作的核心逻辑是基于cogroup进行的：按照key对
		* 所有Value进行扫描然后聚合，每次计算时都需要这样扫描。
		*
		* 好处是：针对RDD进行cogroup操作简单
		* 缺点是：性能低效，需要全局扫描，导致计算时间间隔越大计算速度越慢！！
		*/
	private def computeUsingPrevisousRDD(parentRDD:RDD[(K, V)], prevStateRDD: RDD[(K, S)]):Option[RDD[(K, S)]] = {
		/*
			将函数转换成可以针对CogroupRDD的mapPartitions中的数据进行操
			作的函数
		 */
		val updateFuncLocal = updateFunc
		val finalFunc = (iter: Iterator[(K, (Iterable[V], Iterable[S]))]) => {
			val i = iter.map(t => {
				val itr = t._2._2.iterator
				val headOption = if(itr.hasNext) Some(itr.next()) else None
				(t._1, t._2._1.toSeq, headOption)
			})
			updateFuncLocal(i)
		}

		val cogroupedRDD = parentRDD.cogroup(prevStateRDD, partitioner)
		val stateRDD = cogroupedRDD.mapPartitions(finalFunc, preservePartitioning)
		Some(stateRDD)
	}

}
