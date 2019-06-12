package org.apache.spark.streaming.dstream.state

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, Time}
import org.apache.spark.util.ClosureCleaner

/**
	*  representing all the specifications of the DStream transformation
	*  `mapWithState` operation of a pair DStream[(K,V)]
	*
	* {{{
	*   // A mapping function that maintains an integer state and return a String
	*   def mappingFunction(key: String, value: Option[Int], state: State[Int]): Option[String] = {
	* 		 // Use state.exists(), state.get(), state.update() and state.remove()
	* 		 // to manage state, and return the necessary string
	*		}
	*
	*		val spec = StateSpec.function(mappingFunction).numPartitions(10)
	*		val mapWithStateDStream = keyValueDStream.mapWithState[StateType, MappedType](spec)
	*
	* }}}
	*
	* @tparam KeyType class of the state key
	* @tparam ValueType class of the state value
	* @tparam StateType class of the state data
	* @tparam MappedType class of the mapped elements
	*/
abstract class StateSpec[KeyType, ValueType, StateType, MappedType] extends Serializable {

	/** `mapWithState`的初始状态 */
	def initialState(rdd: RDD[(KeyType, StateType)]): this.type

	/** `mapWithState`产生的RDD的分区数，默认采用HashPartitioner */
	def numPartitions(numPartitions: Int): this.type

	def partitioner(partitioner: Partitioner): this.type

	/** 当某个key-value在`idleDuration`内没有被更新，则认为是idle的，
		* 会通过调用State.isTimingOut()方法将其标记为过期。
		*/
	def timeout(idleDuration: Duration):this.type
}

object StateSpec{
	def function[KeyType, ValueType, StateType, MappedType](
		mappingFunc: (Time, KeyType, Option[ValueType], State[StateType])=>Option[MappedType]
	 ): StateSpec[KeyType, ValueType, StateType, MappedType] = {
		ClosureCleaner.clean(mappingFunc, checkSerializable = true)
		new StateSpecImpl(mappingFunc)
	}

	def function[KeyType, ValueType, StateType, MappedType](
		 mappingFunc: (KeyType, Option[ValueType], State[StateType]) => MappedType
	 ): StateSpec[KeyType, ValueType, StateType, MappedType] = {
		ClosureCleaner.clean(mappingFunc, checkSerializable = true)
		val wrappedFunction =
			(time: Time, key: KeyType, value: Option[ValueType], state: State[StateType]) => {
				Some(mappingFunc(key, value, state))
			}
		new StateSpecImpl(wrappedFunction)
	}
}