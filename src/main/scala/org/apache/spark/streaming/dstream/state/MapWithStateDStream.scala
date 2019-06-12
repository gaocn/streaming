package org.apache.spark.streaming.dstream.state

import org.apache.spark.streaming.SContext
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

abstract class MapWithStateDStream[KeyType, ValueType, StateType, MappedType: ClassTag](ssc: SContext) extends DStream[MappedType](ssc){

	/**
		* return a pair DStream where each RDD is the snapshot
		* of the state of all the keys.
		*/
	def stateSnapshot():DStream[(KeyType, StateType)]
}



class InternalMapWithStateDStream[K:ClassTag, V:ClassTag, S:ClassTag, T:ClassTag](parent:DStream[(K, V)]) extends DStream[()]