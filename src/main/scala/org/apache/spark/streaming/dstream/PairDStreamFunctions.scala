package org.apache.spark.streaming.dstream

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner}

import scala.reflect.ClassTag

class PairDStreamFunctions[K, V](self: DStream[(K, V)])
		(implicit kt: ClassTag[K], vt:ClassTag[V], ord:Ordering[K]) extends Serializable {

	def ssc = self.ssc
	def sc =  self.ssc.sparkContext
	def defaultPartitioner(numPartitions: Int = self.ssc.sc.defaultParallelism) = {
		new HashPartitioner(numPartitions)
	}

	/*==============================
	 *  updateStateByKey
	 *==============================
	 */

	def updateStateByKey[S: ClassTag](updateFunc: (Seq[V], Option[S])=>Option[S]):DStream[(K, S)] = ssc.withScope{
		updateStateByKey(updateFunc, defaultPartitioner())
	}

	def updateStateByKey[S: ClassTag](
		updateFunc: (Seq[V], Option[S])=>Option[S],
		numPartitions: Int):DStream[(K, S)] = ssc.withScope{
		updateStateByKey(updateFunc, defaultPartitioner(numPartitions))
	}

	def updateStateByKey[S: ClassTag](
		 updateFunc: (Seq[V], Option[S])=>Option[S],
		 partitioner: Partitioner):DStream[(K, S)] = ssc.withScope{
		val cleanedUpdateF = sc.clean(updateFunc)
		val newUpdateFunc = (iter: Iterator[(K, Seq[V], Option[S])]) => {
			iter.flatMap(t => cleanedUpdateF(t._2, t._3).map(s => (t._1, s)))
		}
		updateStateByKey(newUpdateFunc, partitioner, true)
	}

	def updateStateByKey[S: ClassTag](
		 updateFunc: Iterator[(K, Seq[V], Option[S])] => Iterator[(K, S)],
		 partitioner: Partitioner,
		 rememberPartitioner: Boolean):DStream[(K, S)] = ssc.withScope{
		new StateDStream(self, ssc.sc.clean(updateFunc),partitioner, rememberPartitioner, None)
	}

	def updateStateByKey[S: ClassTag](
		 updateFunc: (Seq[V], Option[S])=>Option[S],
		 partitioner: Partitioner,
		 initialRDD: RDD[(K, S)]):DStream[(K, S)] = ssc.withScope{
		val cleanedUpdateF = sc.clean(updateFunc)
		val newUpdateFunc = (iter: Iterator[(K, Seq[V], Option[S])]) => {
			iter.flatMap(t => cleanedUpdateF(t._2, t._3).map(s => (t._1, s)))
		}
		updateStateByKey(newUpdateFunc, partitioner, true, initialRDD)
	}

	def updateStateByKey[S: ClassTag](
		 updateFunc: Iterator[(K, Seq[V], Option[S])] => Iterator[(K, S)],
		 partitioner: Partitioner,
		 rememberPartitioner: Boolean,
		 initialRDD: RDD[(K, S)]):DStream[(K, S)] = ssc.withScope{
		new StateDStream(self, ssc.sc.clean(updateFunc),partitioner, rememberPartitioner, Some(initialRDD))
	}

	/*==============================
	 *  mapWithState
	 *==============================
	 */




}
