package org.apache.spark.streaming.dstream

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.state.{MapWithStateDStream, MapWithStateDStreamImpl, StateSpec, StateSpecImpl}
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

	/**
		* 使用一个函数对K-V元素进行状态维护和更新，在更新和维护历史状态时
		* 都是基于Key进行的。更新操作的函数通过StateSpec类进行了封装。
		*
		* {{{
		*
		* 	//下面函数维护一个整型状态，返回字符型数据
		*   def mappingFunction(
		*   		key: String,
		*   		value: Option[Int],
		*   		state: State[Int]): Option[String] = {
		*				//使用state.exists()、state.get()、state.update()
		*				// state.remove()方法更新状态
		*		}
		*
		*		val spec = StateSpec.function(mappingFunction).numPartitions(10)
		*
		*		val mapWithStateDStream = keyValueDStream.mapWithState[StateType, MappedType](spec)
		*
		* }}}
		*
		* 可以将State看做是一个内存数据表，通过API对其进行操作，该表中记
		* 录了状态维护中的所有状态，mappingFunction表示对内存表中的哪个
		* Key对应的记录进行操作，输入的数据为value，通过将输入数据和Key
		* 对应的表中记录的值进行操作得到新的状态，并更新到表中。
		*
		* @param spec Specification of this transformation
		* @tparam StateType Class type of the state data
		* @tparam MappedType Class type of the mapped data
		*/
	def mapWithState[StateType:ClassTag, MappedType:ClassTag](
		 spec: StateSpec[K, V, StateType, MappedType]
	 ):MapWithStateDStream[K,V,StateType,MappedType] = {
		new MapWithStateDStreamImpl[K,V,StateType,MappedType](
			self,
			spec.asInstanceOf[StateSpecImpl[K,V,StateType,MappedType]]
		)
	}

}
