package org.apache.spark.streaming.rdd.state

import java.io.{IOException, ObjectOutputStream}

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

class MapWithStateRDDPartition(
		idx: Int,
		@transient private var prevStateRDD: RDD[_],
		@transient private var partitionedDataRDD: RDD[_]
	) extends Partition{

	private[rdd] var previousSessionRDDPartition: Partition = null
	private[rdd] var partitionedDataRDDPartition: Partition = null

	override def index: Int = idx
	override def hashCode(): Int = idx

	@throws(classOf[IOException])
	private def writeObject(oos: ObjectOutputStream):Unit = Utils.tryOrIOException{
		//Update the reference to parent split at the time of task serialization
		previousSessionRDDPartition = prevStateRDD.partitions(index)
		partitionedDataRDDPartition = partitionedDataRDD.partitions(index)
		oos.defaultWriteObject()
	}
}
