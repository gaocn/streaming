package org.apache.spark.streaming.rdd.state

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.state.State

import scala.reflect.ClassTag

/**
	* RDD storing the keyed states of `mapWithState` operation and
	*  corresponding mapped data.
	*
	* Each partition of this RDD has a single record of type
	* [[MapWithStateRDDRecord]]. This contains a [[StateMap]]
	* (containing the keyed-states) and the sequence of records
	* returned by the mapping function of  `mapWithState`.
	*
	*
	* @param prevStateRDD he previous MapWithStateRDD on whose
	*          StateMap data `this` RDD will be created
	* @param partitionedDataRDD The partitioned data RDD which
	*          is used update the previous StateMaps in the
	*          `prevStateRDD` to create `this` RDD
	* @param mappingFunc The function that will be used to update
	*          state and return new data
	* @param batchTime The time of the batch to which this RDD
	*          belongs to. Use to update
	* @param timeoutThresholdTime The time to indicate which keys
	*          are timeout
	* @tparam K class type of key
	* @tparam V class type of value
	* @tparam S class type of state data
	* @tparam E class type of mapped state
	*/
class MapWithStateRDD[K:ClassTag, V:ClassTag,S:ClassTag, E:ClassTag](
		private var prevStateRDD: RDD[MapWithStateRDDRecord[K,S,E]],
		private var partitionedDataRDD: RDD[(K, V)],
		mappingFunc: (Time, K, Option[V], State[S])=>Option[E],
		batchTime: Time,
		timeoutThresholdTime: Option[Long]
	) extends RDD[MapWithStateRDDRecord[K,S,E]](
		partitionedDataRDD.sparkContext,
		List(
			new OneToOneDependency[MapWithStateRDDRecord[K,S,E]](prevStateRDD),
			new OneToOneDependency(partitionedDataRDD)
		)
	){

	@volatile
	private var doFullScan = false

	require(prevStateRDD.partitioner.nonEmpty)
	require(partitionedDataRDD.partitioner == prevStateRDD.partitioner)

	override val partitioner: Option[Partitioner] = prevStateRDD.partitioner

	override def checkpoint(): Unit = {
		super.checkpoint()
		doFullScan = true
	}

	override protected def getPartitions: Array[Partition] = {
		Array.tabulate(prevStateRDD.partitions.length){i=>
			new MapWithStateRDDPartition(i, prevStateRDD, partitionedDataRDD)
		}
	}

	override def clearDependencies(): Unit = {
		super.clearDependencies()
		prevStateRDD = null
		partitionedDataRDD = null
	}

	def setFullScan(): Unit = {
		doFullScan = true
	}

	override def compute(partition: Partition, context: TaskContext): Iterator[MapWithStateRDDRecord[K, S, E]] = {
		val stateRDDPartition = partition.asInstanceOf[MapWithStateRDDPartition]
		val preStateRDDIterator = prevStateRDD.iterator(stateRDDPartition.previousSessionRDDPartition, context)
		val dataIterator = partitionedDataRDD.iterator(stateRDDPartition.partitionedDataRDDPartition,context)

		val prevRecord = if(preStateRDDIterator.hasNext)Some(preStateRDDIterator.next())else None
		val newRecord = MapWithStateRDDRecord.updateRecordWithData(
			prevRecord,
			dataIterator,
			mappingFunc,
			batchTime,
			timeoutThresholdTime,
			removeTimedoutData = doFullScan //只有在full scan启用时才允许移除timedout data
		)

		Iterator(newRecord)
	}
}

object MapWithStateRDD {

	def createFromPairRDD[K:ClassTag, V:ClassTag, S:ClassTag,E:ClassTag](
		pairRDD: RDD[(K, S)],
		partitioner: Partitioner,
		updateTime:Time
	):MapWithStateRDD[K,V,S,E] =  {

		val stateRDD = pairRDD.partitionBy(partitioner).mapPartitions({iter=>
				val stateMap = StateMap.create[K,S](SparkEnv.get.conf)
				iter.foreach{case (key, state)=>
					stateMap.put(key, state, updateTime.milliseconds)
				}
			Iterator(MapWithStateRDDRecord(stateMap, Seq.empty[E]))
		}, preservesPartitioning = true)

		val emptyDataRDD = pairRDD.sparkContext.emptyRDD[(K,V)].partitionBy(partitioner)

		val noOpFunc = (time:Time, key:K, value:Option[V], state:State[S])=>None

		new MapWithStateRDD[K,V,S,E](stateRDD, emptyDataRDD, noOpFunc, updateTime,None)
	}

	def createFromRDD[K:ClassTag, V:ClassTag, S:ClassTag,E:ClassTag](
			rdd:RDD[(K, S, Long)],
			partitioner: Partitioner,
			updateTime: Time
		):MapWithStateRDD[K,V,S,E] =  {

		val pairRDD  = rdd.map{x=> (x._1, (x._2, x._3))}
		val stateRDD = pairRDD.partitionBy(partitioner).mapPartitions({iter=>
			val stateMap = StateMap.create[K,S](SparkEnv.get.conf)
			iter.foreach{case (key, (state, updateTime)) =>
				stateMap.put(key, state, updateTime)
			}
			Iterator(MapWithStateRDDRecord(stateMap, Seq.empty[E]))
		}, preservesPartitioning = true)

		val emptyDataRDD = pairRDD.sparkContext.emptyRDD[(K, V)].partitionBy(partitioner)

		val noOpFunc = (time: Time, key: K, value: Option[V], state: State[S]) => None

		new MapWithStateRDD[K, V, S, E](
			stateRDD, emptyDataRDD, noOpFunc, updateTime, None)
	}
}