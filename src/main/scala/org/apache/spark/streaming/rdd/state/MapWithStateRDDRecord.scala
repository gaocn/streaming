package org.apache.spark.streaming.rdd.state

import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.state.{State, StateImpl}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
	* 代表[[MapWithStateRDD]]中的一条记录
	*  Each record contains a [[StateMap]] and a  sequence of
	*  records returned by the mapping function of `mapWithState`.
	*/
case class MapWithStateRDDRecord[K,S,E](
		 var stateMap:StateMap[K, S],
		 var mappedData:Seq[E])

object MapWithStateRDDRecord {

	def updateRecordWithData[K:ClassTag, V:ClassTag, S:ClassTag, E:ClassTag](
			prevRecord: Option[MapWithStateRDDRecord[K, S, E]],
			dataIter: Iterator[(K, V)], //当前Batch的数据
			mappingFunc: (Time, K, Option[V], State[S]) => Option[E],
			batchTime: Time,            //当前Batch Time
			timeoutThresholdTime: Option[Long],
			removeTimedoutData: Boolean
		):MapWithStateRDDRecord[K, S, E] =  {

		//若之前存在StateMap则拷贝一个，否则创建一个空的
		val newStateMap = prevRecord.map(_.stateMap.copy()).getOrElse(new EmptyStateMap[K,S])

		//代表最后要返回的值
		val mappedData = new ArrayBuffer[E]
		val wrappedState = new StateImpl[S]()

		/*
			遍历当前Batch中的所有数据，使用自定义函数对当前Batch中的数据进
			行计算，并更新到newStateMap中，该数据结构保存了所有历史状态。

			这里不需要对历史状态进行重新计算或遍历，而是根据当前Batch数据更
			新历史状态(类似数据表的更新操作)，因此mapWithState较updateStateByKey
			效率会更高。
		 */
		dataIter.foreach{case (key, value) =>
			wrappedState.wrap(newStateMap.get(key))
			val returned = mappingFunc(batchTime, key, Some(value), wrappedState)
			if(wrappedState.isRemoved()) {
				newStateMap.remove(key)
			} else if(wrappedState.isUpdated() || (wrappedState.exists() && timeoutThresholdTime.isDefined)) {
				newStateMap.put(key, wrappedState.get(), batchTime.milliseconds)
			}
			mappedData ++= returned
		}

		//获取超时的state records
		if(removeTimedoutData && timeoutThresholdTime.isDefined) {
			newStateMap.getByTime(timeoutThresholdTime.get).foreach{case (key, state, _) =>
				wrappedState.wrapTimingOutState(state)
				val returned = mappingFunc(batchTime, key, None, wrappedState)
				mappedData ++= returned
				newStateMap.remove(key)

			}
		}

		/*
		 	每个时间窗口进行状态更新，这个RDD的Partition没有变，但是Partition
		 	内部的数据变了。虽然RDD不可变，但在RDD内部封装的数据结构的内容
		 	可以改变。因此它借助RDD的不变性同时整合的数据结构的可变特征。

		 	问题：Spark能不能处理变化的数据？
		 	RDD本身不可变，但是可以处理变化的数据，只不过需要自定义RDD内部
		 	的数据结构。

		 	观点：RDD不可变，所以他处理的数据就是不可变的？
		 	该观点后半部分错误，数据源可以变化，只不过需要我们自己维护或改变
		 	这些数据。
		 */

		MapWithStateRDDRecord(newStateMap, mappedData)
	}
}
