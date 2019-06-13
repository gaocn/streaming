package org.apache.spark.streaming.dstream.state

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, SContext, Time}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.rdd.state.{MapWithStateRDD, MapWithStateRDDRecord}
import InternalMapWithStateDStream._

import scala.reflect.ClassTag

abstract class MapWithStateDStream[KeyType, ValueType, StateType, MappedType: ClassTag](ssc: SContext) extends DStream[MappedType](ssc){

	/**
		* return a pair DStream where each RDD is the snapshot
		* of the state of all the keys.
		*/
	def stateSnapshot():DStream[(KeyType, StateType)]
}

class MapWithStateDStreamImpl[KeyType: ClassTag, ValueType: ClassTag, StateType: ClassTag, MappedType: ClassTag](
		dataStream: DStream[(KeyType, ValueType)],
		spec: StateSpecImpl[KeyType, ValueType, StateType, MappedType]
	)extends MapWithStateDStream[KeyType, ValueType, StateType, MappedType](dataStream.ssc){

	private val internalStream = new InternalMapWithStateDStream[KeyType, ValueType, StateType, MappedType](dataStream, spec)

	override def compute(validTime: Time): Option[RDD[MappedType]] = {
		internalStream.getOrCompute(validTime).map(_.flatMap[MappedType](_.mappedData))
	}

	override def dependencies: List[DStream[_]] = List(internalStream)

	override def slideDuration: Duration = internalStream.slideDuration

	/**
		* Forward the checkpoint interval to the internal DStream
		* that computes the state maps. This to make sure that
		* this DStream does not get checkpointed, only the
		* internal stream.
		*/
	override def checkpoint(interval: Duration): DStream[MappedType] = {
		internalStream.checkpoint(interval)
		this
	}

	/**
		* Return a pair DStream where each RDD is the snapshot of
		* the state of all the keys.
		*/
	override def stateSnapshot(): DStream[(KeyType, StateType)] = {
		internalStream.flapMap{
			_.stateMap.getAll().map{case (k,s,_)=>(k,s)}.toTraversable
		}
	}

	def keyClass:Class[_] = implicitly[ClassTag[KeyType]].runtimeClass
	def valueClass:Class[_] = implicitly[ClassTag[ValueType]].runtimeClass
	def stateClass:Class[_] = implicitly[ClassTag[StateType]].runtimeClass
	def MappedClass:Class[_] = implicitly[ClassTag[MappedType]].runtimeClass
}

/**
	* A DStream that allows per-key state to be maintains, and
	* arbitrary records to be generated based on updates to the
	*  state. This is the main DStream that implements the `mapWithState`
	* operation on DStreams.
	*
	* @param parent Parent (key, value) stream that is the source
	* @param spec   Specifications of the mapWithState operation
	*/
class InternalMapWithStateDStream[K: ClassTag, V: ClassTag, S: ClassTag, E: ClassTag](parent: DStream[(K, V)], spec: StateSpecImpl[K, V, S, E]) extends DStream[MapWithStateRDDRecord[K, S, E]](parent.ssc) {

	/**
		* 持久化方式：内存缓存
		* 为什么能保证MEMORY_ONLY方式？
		*	因为基于内存数据结构，并且不断更新该内存数据结构而不是创建新的对象。
		*/
	persist(StorageLevel.MEMORY_ONLY)

	private val mappingFunc = spec.getFunction()
	private val partitioner  = spec.getPartitioner().getOrElse(
		new HashPartitioner(ssc.sc.defaultParallelism)
	)

	/** enable automatic checkpointing */
	override val mustCheckpoint = true

	/** 覆写checkpoint duration */
	override def initialize(time: Time): Unit = {
		if(checkpointDuration == null) {
			checkpointDuration = slideDuration * DEFAULT_CHECKPOINT_DURATION_MULTIPLIER
		}
		super.initialize(time)
	}

	override def dependencies: List[DStream[_]] = List(parent)

	override def slideDuration: Duration = parent.slideDuration

	override def compute(validTime: Time): Option[RDD[MapWithStateRDDRecord[K, S, E]]] = {
		//获取之前的或创建空的RDD
		val prevStateRDD = getOrCompute(validTime - slideDuration) match {
			case Some(rdd) =>
				if(rdd.partitioner != Some(partitioner)) {
					//确保RDD按照正确分区器进行分区
					MapWithStateRDD.createFromRDD[K,V,S,E](rdd.flatMap(_.stateMap.getAll()),partitioner, validTime)
				} else  {
					rdd
				}
			case None =>
				MapWithStateRDD.createFromPairRDD[K,V,S,E](
					spec.getInitialStateRDD().getOrElse(new EmptyRDD[(K, S)](ssc.sc)),
					partitioner,
					validTime
				)
		}

		//根据历史状态和最新batch中的数据，更新状态
		val dataRDD = parent.getOrCompute(validTime).getOrElse{
			context.sparkContext.emptyRDD[(K,V)]
		}
		val partitionedDataRDD = dataRDD.partitionBy(partitioner)
		val timeoutThresholdTime = spec.getTimeoutInterval().map{interval =>
			(validTime - interval).milliseconds
		}

		Some(new MapWithStateRDD(prevStateRDD, partitionedDataRDD, mappingFunc,validTime,timeoutThresholdTime))
	}
}
private[streaming] object InternalMapWithStateDStream {
	private val DEFAULT_CHECKPOINT_DURATION_MULTIPLIER = 10
}
