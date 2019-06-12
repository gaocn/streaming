package org.apache.spark.streaming.dstream.state

import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, Time}

class StateSpecImpl[K, V, S, T](
		function:(Time, K, Option[V], State[S])=>Option[T]
	 ) extends StateSpec[K, V, S, T] {
	require(function != null)

	@volatile private var partitioner: Partitioner = null
	@volatile private var initialStateRDD: RDD[(K, S)] = null
	@volatile private var timeoutInterval:Duration  = null



	/** `mapWithState`的初始状态 */
	override def initialState(rdd: RDD[(K, S)]): StateSpecImpl.this.type = {
		this.initialStateRDD = rdd
		this
	}

	/** `mapWithState`产生的RDD的分区数，默认采用HashPartitioner */
	override def numPartitions(numPartitions: Int): StateSpecImpl.this.type = {
		this.partitioner = new HashPartitioner(numPartitions)
		this
	}

	override def partitioner(partitioner: Partitioner): StateSpecImpl.this.type = {
		this.partitioner = partitioner
		this
	}

	/** 当某个key-value在`idleDuration`内没有被更新，则认为是idle的，
		* 会通过调用State.isTimingOut()方法将其标记为过期。
		*/
	override def timeout(idleDuration: Duration): StateSpecImpl.this.type = {
		this.timeoutInterval = idleDuration
		this
	}

	/*
	 * private methods
	 */
	private def getFunction():(Time, K, Option[V], State[S])=>Option[T] = function
	private def getInitialStateRDD(): Option[RDD[(K, S)]] = Option(initialStateRDD)
	private def getPartitioner(): Option[Partitioner] = Option(partitioner)
	private def getTimeoutInterval(): Option[Duration] = Option(timeoutInterval)
}
