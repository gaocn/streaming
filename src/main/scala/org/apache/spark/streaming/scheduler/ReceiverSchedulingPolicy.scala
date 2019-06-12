package org.apache.spark.streaming.scheduler

import org.apache.spark.scheduler.{ExecutorCacheTaskLocation, TaskLocation}
import org.apache.spark.streaming.domain.ReceiverTrackingInfo
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.scheduler.ReceiverState._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
	* Receiver调度策略：尽量让receiver均匀的分布到每个Executor上。调
	* 度分为两个阶段：
	*
	*【第一阶段：global scheduling】
	* 当ReceiverTracker启动时，会同时对所有receivers执行调度策略。
	* 1、调用`scheduleReceivers`方法将receivers分均匀布，根据返回结
	* 果更新`receiverTrackingInfoMap`。同时每个receiver的`ReceiverTrackingInfo.scheduledLocations`
	* 的值应该设置为被分配的位置。
	*
	* 2、当receiver启动时，会发送一条注册消息给ReceiverTracker，在接
	* 收到消息后，会调用`ReceiverTracker.registerReceiver`方法，该
	* 方法会检查该receiver中的scheduledLocations是否是已被分配的位置
	* 中的一个，若不是则会拒绝receiver的注册。
	*
	*【第二阶段：local scheduling】
	* 当receiver重启时被调用，receiver重启有两种情况：
	* 1、因为receiver启动位置与被分配的位置不匹配而重启时，`ReceiverTracker`
	* 会从被分配的位置列表中找到active的executor，然后这些可用的executor
	* 启动receiver job。
	*
	* 2、因为receiver被分配的位置列表为空或该位置列表中的executor都已
	* 经挂掉了而重启时，`ReceiverTracker`应该将`ReceiverTrackingInfo.scheduledLocations`
	* 信息清空，以后当receiver注册时，因为位置信息为空，我们就知道这是
	* 一个local scheduling，然后重新调用`rescheduleReceiver`方法检
	* 查启动receiver的位置是否匹配。
	*/
class ReceiverSchedulingPolicy {

	/**
		* 【global scheduling】
		* 尽量将receivers均匀分布到executors中，但若由于receivers的
		* `preferedLocation`本来就不均匀，那么我们就无法均匀的分配receviers。
		*
		* 算法：
		*  1、按照每个receivers的`preferedLocation`，将这些hosts上的
		*   executors平均分配receivers
		*  2、将其他没有`preferedLocation`的receivers平均分配到所有的
		*   executors上。
		*
		* @return a map for receivers and their scheduled locations
		*/
	def scheduleReceivers(
			 receivers:Seq[Receiver[_]],
			 executors: Seq[ExecutorCacheTaskLocation]):Map[Int, Seq[TaskLocation]] = {
		if(receivers.isEmpty) {
			return Map.empty
		}

		if(executors.isEmpty) {
			return receivers.map(_.streamId -> Seq.empty).toMap
		}


		val hostToExecutors =executors.groupBy(_.host)
		val scheduledLocations = Array.fill(receivers.length)(new ArrayBuffer[TaskLocation])
		val numReceiversOnExecutor = mutable.HashMap[ExecutorCacheTaskLocation, Int]()
		//初始化
		executors.foreach(e => numReceiversOnExecutor(e) = 0)

		//根据每个receivers的`preferedLocation`，确保将其设置为该receiver的候选的分配位置
		for (i <- 0 until receivers.length) {
			//preferredLocation is host but executors are host_executorId
			receivers(i).preferredLocation.foreach{host=>
				hostToExecutors.get(host) match {
					case Some(executorsOnHost) =>
						// Select an executor that has the least receivers in this host
						val leastScheduledExecutor = executorsOnHost.minBy(executor=>numReceiversOnExecutor(executor))
						scheduledLocations(i) += leastScheduledExecutor
						numReceiversOnExecutor(leastScheduledExecutor) = numReceiversOnExecutor(leastScheduledExecutor) + 1
					case None =>
						//preferredLocation is an unknown host.有两种情况：
						//1、executor尚未启动，但晚点可能会被启动
						//2、executor挂掉或者该host不是集群中的一个节点
						//由于host可能是HDFSCacheTaskLocation，因此这里采用TaskLocation处理
						scheduledLocations(i) += TaskLocation(host)
				}
			}
		}

		//对于没有`preferedLocation`的receivers，确保至少给它们分配一个executor
		for(scheduledLocationsForOneReceiver <- scheduledLocations.filter(_.isEmpty)) {
			//Select the executor that has the least receivers
			val(leastScheduledExecutor, numReceivers) = numReceiversOnExecutor.minBy(_._2)
			scheduledLocationsForOneReceiver += leastScheduledExecutor
			numReceiversOnExecutor(leastScheduledExecutor) = numReceivers + 1
		}

		//Assign idle executors to receivers that have less executors
		val idleExecutors = numReceiversOnExecutor.filter(_._2 == 0).map(_._1)
		for(executor <- idleExecutors) {
			//Assign an idle executor to the receiver that has least candidate executors
			val leastScheduledExecutors = scheduledLocations.minBy(_.size)
			leastScheduledExecutors += executor
		}
		receivers.map(_.streamId).zip(scheduledLocations).toMap
	}

	/**
		*【local scheduling】
		* 为当前receiver分配能够运行该receiver的位置，若返回列表为空，则
		* receiver可以运行在任何一个executor上。
		*
		* 该方法会尽量做到executor的负载均衡，为当前receiver分配运行位
		* 置的算法如下：
		*
		* 1、若preferredLocation不为空，则其应该后候选位置之一；
		* 2、根据每个executor上运行的receiver情况，为每个executor分配
		* 一个权值：
		* 	1)、若有一个receiver在executor上运行，则对executor贡献
		* 		的权值为1.0
		*		2)、若有receiver被分配给executor，但其上尚未运行任何executor
		*			时，此时其权值为：`1.0 / #candidate_executors_of_this_receiver`
		*
		*		若有空闲的executor(权值为0)，则返回所有空闲的executor，否
		*		则返回权值最小的executors。
		*
		* 该方法一般在receiver注册或receiver重启时被调用。
		*
		*/
	def rescheduleReceiver(
					receiverId:Int,
					preferredLocation:Option[String],
					receiverTrackingInfoMap:mutable.HashMap[Int, ReceiverTrackingInfo],
					executors: Seq[ExecutorCacheTaskLocation]):Seq[TaskLocation] = {
		if(executors.isEmpty) {
			return Seq.empty
		}

		//always try to schedule to the preferred locations
		val scheduledLocactions = mutable.Set[TaskLocation]()
		//Note: preferredLocation could be `HDFSCacheTaskLocation`,
		// so use `TaskLocation.apply` to handle this case
		scheduledLocactions ++= preferredLocation.map(TaskLocation(_))

		val executorWeights: Map[ExecutorCacheTaskLocation, Double] = {
			receiverTrackingInfoMap.values.flatMap(convertReceiverTrackingInfoToExecutorWeights)
  			.groupBy(_._1).mapValues(_.map(_._2).sum)
		}

		val idleExecutors = executors.toSet -- executorWeights.keys
		if(idleExecutors.nonEmpty) {
			scheduledLocactions ++= idleExecutors
		} else {
			//没有空闲的executor，因此选择所有权值最小的executors
			val sortedExecutors = executorWeights.toSeq.sortBy(_._2)
			if(sortedExecutors.nonEmpty) {
				val minWeight = sortedExecutors(0)._2
				scheduledLocactions ++= sortedExecutors.takeWhile(_._2 == minWeight).map(_._1)
			} else {
				//this should not happen，since executor is not empty
			}
		}

		scheduledLocactions.toSeq
	}

	private def convertReceiverTrackingInfoToExecutorWeights(receiverTrackingInfo: ReceiverTrackingInfo):Seq[(ExecutorCacheTaskLocation,Double)] = {
		receiverTrackingInfo.state match {
			case INACTIVE => Nil
			case SCHEDULED =>
				val scheduledLocations =  receiverTrackingInfo.scheduledLocations.get
				//权值为receiver运行在被分配的executors中的一个的概率
				scheduledLocations.filter(_.isInstanceOf[ExecutorCacheTaskLocation])
  				.map{location =>
						location.asInstanceOf[ExecutorCacheTaskLocation] -> (1.0 / scheduledLocations.size)
					}
			case ACTIVE =>
				Seq(receiverTrackingInfo.runningExecutor.get -> 1.0)
		}
	}
}
