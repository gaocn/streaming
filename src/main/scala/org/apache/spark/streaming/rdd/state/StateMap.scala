package org.apache.spark.streaming.rdd.state

import java.io.{ObjectInput, ObjectInputStream, ObjectOutput, ObjectOutputStream}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.apache.spark.SparkConf
import org.apache.spark.serializer.{KryoInputObjectInputBridge, KryoOutputObjectOutputBridge}
import org.apache.spark.util.collection.OpenHashMap
import OpenHashMapBasedStateMap._
import scala.reflect.ClassTag

/**
	* keeps track of sessions
	*
	* @tparam K class of the state kay
	* @tparam S class of the state data
	*/
abstract class StateMap[K, S] extends Serializable {

	def get(key: K): Option[S]

	/** 获取updatedTime在threshUpdatedTime之前的所有Keys、Stats*/
	def getByTime(threshUpdatedTime: Long):Iterator[(K, S, Long)]

	def getAll(): Iterator[(K, S, Long)]

	def put(key: K, state: S, updatedTime: Long):Unit

	def remove(key: K):Unit

	/** shallow copy, update of the new map should not mutate this map */
	def copy(): StateMap[K, S]

	def toDebugString():String = toString
}

object StateMap {
	def empty[K, S]: StateMap[K, S] = new EmptyStateMap[K, S]

	def create[K:ClassTag, S:ClassTag](conf: SparkConf): StateMap[K, S] = {
		val deltaChainThreshold = conf.getInt("spark.streaming.sessionByKey.deltaChainThreshold", DELTA_CHAIN_LENGTH_THRESHOLD)
		new OpenHashMapBasedStateMap[K,S](deltaChainThreshold)
	}
}

/*
 * Empty State Map
 */
class EmptyStateMap[K, S] extends StateMap[K, S] {
	override def get(key: K): Option[S] = None
	override def getByTime(threshUpdatedTime: Long): Iterator[(K, S, Long)] = Iterator.empty
	override def getAll(): Iterator[(K, S, Long)] = Iterator.empty
	override def put(key: K, state: S, updatedTime: Long): Unit = {
		throw new NotImplementedError("put方法不应该被调用")
	}
	override def remove(key: K): Unit = {}
	override def copy(): StateMap[K, S] = this
	override def toDebugString(): String = ""
}


/*
 * Open HashMap Based StateMap
 */

class OpenHashMapBasedStateMap[K, S](
		@transient @volatile var parentStateMap: StateMap[K, S],
		private var initialCapacity: Int = DEFAULT_INITIAL_CAPACITY,
		private var deltaChainThreshold: Int = DELTA_CHAIN_LENGTH_THRESHOLD
	)(
		implicit private var keyClassTag: ClassTag[K], private var stateClassTag:ClassTag[S]
	) extends StateMap[K,S] with KryoSerializable{self=>
	/*=====================================
	 * Constructs
	 *=====================================
	 */
	def this(initialCapacity: Int, deltaChainThreshold: Int)
					(implicit keyClassTag:ClassTag[K], stateClassTag: ClassTag[S]) = this(
		new EmptyStateMap[K, S], initialCapacity, deltaChainThreshold
	)

	def this(deltaChainThreshold: Int)
					(implicit keyClassTag:ClassTag[K], stateClassTag: ClassTag[S]) = this(
		DEFAULT_INITIAL_CAPACITY, deltaChainThreshold
	)

	def this()
					(implicit keyClassTag:ClassTag[K], stateClassTag: ClassTag[S]) = this(
		 DELTA_CHAIN_LENGTH_THRESHOLD
	)

	require(initialCapacity >= 1, "invalid initial capacity ")
	require(deltaChainThreshold >= 1, "invalid delta chain threshold")

	@transient
	@volatile
	private var deltaMap = new OpenHashMap[K, StateInfo[S]]()

	/*=====================================
	 * StateMap Methods
	 *=====================================
	 */

	override def get(key: K): Option[S] = {
		val stateInfo = deltaMap(key)
		if(stateInfo != null) {
			if(!stateInfo.deleted){
				Some(stateInfo.data)
			} else {
				None
			}
		} else {
			parentStateMap.get(key)
		}
	}

	/** 获取updatedTime在threshUpdatedTime之前的所有Keys、Stats */
	override def getByTime(threshUpdatedTime: Long): Iterator[(K, S, Long)] = {
		val oldStates = parentStateMap.getByTime(threshUpdatedTime)
  		.filter{case (key, value, _)=> !deltaMap.contains(key)}

		val updatedStates = deltaMap.iterator.filter{case (_, stateInfo) =>
			!stateInfo.deleted && stateInfo.updateTime <  threshUpdatedTime
		}.map{case(key, stateInfo) =>
			(key, stateInfo.data, stateInfo.updateTime)
		}
		oldStates ++ updatedStates
	}

	override def getAll(): Iterator[(K, S, Long)] = {
		val oldStates = parentStateMap.getAll().filter{case (key, _, _) =>
			!deltaMap.contains(key)
		}

		val updatedStates = deltaMap.iterator.filter{!_._2.deleted}
  		.map{case(key, stateInfo)=> (key, stateInfo.data, stateInfo.updateTime)}

		oldStates ++ updatedStates
	}

	override def put(key: K, state: S, updatedTime: Long): Unit = {
		val stateInfo = deltaMap(key)
		if(stateInfo != null) {
			stateInfo.update(state, updatedTime)
		} else {
			deltaMap.update(key, new StateInfo(state, updatedTime))
		}
	}

	override def remove(key: K): Unit = {
		val stateInfo = deltaMap(key)
		if(stateInfo != null) {
			stateInfo.markDeleted
		} else {
			val newInfo = new StateInfo[S](deleted = true)
			deltaMap.update(key, newInfo)
		}
	}

	/** shallow copy, update of the new map should not mutate this map */
	override def copy(): StateMap[K, S] = {
		new OpenHashMapBasedStateMap[K,S](this, deltaChainThreshold)
	}

	def shouldCompact: Boolean = deltaChainLength >= deltaChainThreshold

	def deltaChainLength:Int = parentStateMap match {
		case map: OpenHashMapBasedStateMap[_,_] => map.deltaChainLength + 1
		case _ => 0
	}

	def approxSize:Int = deltaMap.size + {
		parentStateMap match {
			case map:OpenHashMapBasedStateMap[_,_] => map.approxSize
			case _ => 0
		}
	}

	override def toDebugString(): String = {
		val tabs = if(deltaChainLength > 0) {
			("    " * (deltaChainLength - 1)) + "+---"
		} else ""

		parentStateMap.toDebugString() + "\n" + deltaMap.iterator.mkString(tabs, "\n" + tabs, "")
	}

	override def toString: String = {
		s"[${System.identityHashCode(this)}, ${System.identityHashCode(parentStateMap)}]"
	}

	/*=====================================
	 * ObjectOutputStream/ObjectInputStream
	 *=====================================
	 */
	/** 序列化map数据，如果需要还会进行delta压缩 */
	private def writeObjectInternal(outputStream: ObjectOutput):Unit = {
		//1. write data in the delta map of this state map
		outputStream.writeInt(deltaMap.size)
		val deltaMapIter = deltaMap.iterator
		var deltaMapCount = 0
		while (deltaMapIter.hasNext) {
			deltaMapCount += 1
			val (key, stateInfo) = deltaMapIter.next()
			outputStream.writeObject(key)
			outputStream.writeObject(stateInfo)
		}
		assert(deltaMapCount == deltaMap.size)

		//2. 若需要压缩，则将parent state map先拷贝到新的map中进行压缩后再序列化
		val doCompaction = shouldCompact
		val newParentSessionStore = if(doCompaction) {
			val initCapacity = if(approxSize > 0) approxSize else 64
			new OpenHashMapBasedStateMap[K, S](initCapacity, deltaChainThreshold)
		} else {null}

		val activeSessionIter = parentStateMap.getAll()
		var parentSessionCount =0

		//以便反序列化时分配改长度的空间
		outputStream.writeInt(approxSize)

		while (activeSessionIter.hasNext) {
			parentSessionCount += 1

			val (key, stateInfo, updateTime) = activeSessionIter.next()
			outputStream.writeObject(key)
			outputStream.writeObject(stateInfo)
			outputStream.writeObject(updateTime)

			if(doCompaction) {
				newParentSessionStore.deltaMap.update(key, StateInfo(stateInfo, updateTime, false))
			}
		}

		//添加LimitMarker标识，用parentSessionCount作为标识
		val limitObj = new LimitMarker(parentSessionCount)
		outputStream.writeObject(limitObj)
		if(doCompaction) {
			parentStateMap = newParentSessionStore
		}
	}

	//反序列化
	private def  readObjectInternal(inputStream: ObjectInput):Unit = {
		val deltaMapSize = inputStream.readInt()
		deltaMap = if(deltaMapSize != 0) {
			new OpenHashMap[K, StateInfo[S]](deltaMapSize)
		} else {
			new OpenHashMap[K, StateInfo[S]](initialCapacity)
		}

		var deltaMapCount = 0
		while (deltaMapCount < deltaMapSize)  {
			val key = inputStream.readObject().asInstanceOf[K]
			val sessionInfo = inputStream.readObject().asInstanceOf[StateInfo[S]]
			deltaMap.update(key, sessionInfo)
			deltaMapCount += 1
		}

		val parentStateMapSize = inputStream.readInt()
		val newStateMapInitialCapacity  = math.max(parentStateMapSize, DEFAULT_INITIAL_CAPACITY)
		val newParentSessionStore = new OpenHashMapBasedStateMap[K, S](newStateMapInitialCapacity,deltaChainThreshold)

		var parentSessionLoopDone = false
		while (!parentSessionLoopDone) {
			val obj = inputStream.readObject()
			if(obj.isInstanceOf[LimitMarker]) {
				parentSessionLoopDone = true
				val expectedCount = obj.asInstanceOf[LimitMarker].num
				assert(expectedCount == newParentSessionStore.deltaMap.size)
			} else {
				val key =  inputStream.readObject().asInstanceOf[K]
				val state = inputStream.readObject().asInstanceOf[S]
				val updateTime = inputStream.readLong()
				newParentSessionStore.deltaMap.update(key, StateInfo(state, updateTime, false))
			}
		}
		parentStateMap = newParentSessionStore
	}

	private def writeObject(outputStream: ObjectOutputStream):Unit = {
		// Write all the non-transient fields, especially class tags, etc.
		outputStream.defaultWriteObject()
		writeObjectInternal(outputStream)
	}

	private def readObject(inputStream: ObjectInputStream): Unit = {
		inputStream.defaultReadObject()
		readObjectInternal(inputStream)
	}

	/*=====================================
	 * Kryo Serializer
	 *=====================================
	 */
	override def write(kryo: Kryo, output: Output): Unit = {
		output.writeInt(initialCapacity)
		output.writeInt(deltaChainThreshold)
		kryo.writeClassAndObject(output, keyClassTag)
		kryo.writeClassAndObject(output, stateClassTag)
		writeObjectInternal(new KryoOutputObjectOutputBridge(kryo, output))
	}

	override def read(kryo: Kryo, input: Input): Unit = {
		initialCapacity = input.readInt()
		deltaChainThreshold = input.readInt()
		keyClassTag = kryo.readClassAndObject(input).asInstanceOf[ClassTag[K]]
		stateClassTag =kryo.readClassAndObject(input).asInstanceOf[ClassTag[S]]
		readObjectInternal(new KryoInputObjectInputBridge(kryo, input))
	}
}

object OpenHashMapBasedStateMap {

	/**
		* represent the state information
		*/
	case class StateInfo[S](
		var data: S = null.asInstanceOf[S],
		var  updateTime: Long = -1,
		var deleted: Boolean = false
	 ) {

		def markDeleted:Unit = {
			deleted  = true
		}

		def update(newData: S, newUpdateTime:Long):Unit = {
			this.data = newData
			this.updateTime =newUpdateTime
			deleted = false
		}
	}

	/**
		* represent a marker the demarkate the the end of all
		* state data in the serialized bytes
		*/
	class LimitMarker(val num: Int) extends Serializable

	val DEFAULT_INITIAL_CAPACITY  = 64
	val DELTA_CHAIN_LENGTH_THRESHOLD  = 20
}