package org.apache.spark.streaming.domain

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.{ExecutorCacheTaskLocation, TaskLocation}
import org.apache.spark.streaming.scheduler.ReceiverState
import org.apache.spark.streaming.scheduler.ReceiverState.ReceiverState

case class ReceiverErrorInfo(
		lastErrorMsg:String = "",
		lastError: String = "",
		lastErrorTime:Long = -1L)

/**
	* receiver的相关信息
	*
	* @param receiverId receiver的唯一标识
	* @param state 当前receiver的状态
	* @param scheduledLocations 通过[[org.apache.spark.streaming.scheduler.ReceiverSchedulingPolicy]]
	*                           调度后，receiver的位置
	* @param runningExecutor receiver运行时所在的executor
	* @param name receiver的名称
	* @param endpoint receiver endpoint，用于向receiver发送消息
	* @param errorInfo 若receiver失败，则失败的相关信息
	*/
case class ReceiverTrackingInfo(
		 receiverId:Int,
		 state:ReceiverState,
		 scheduledLocations:Option[Seq[TaskLocation]],
		 runningExecutor:Option[ExecutorCacheTaskLocation],
		 name: Option[String] = None,
		 endpoint: Option[RpcEndpointRef] = None,
		 errorInfo: Option[ReceiverErrorInfo] = None) {

	def toReceiverInfo:ReceiverInfo = {
		ReceiverInfo(
			receiverId,
			name.getOrElse(""),
			state == ReceiverState.ACTIVE,
			location = runningExecutor.map(_.host).getOrElse(""),
			executorId = runningExecutor.map(_.executorId).getOrElse(""),
			lastErrorMessage = errorInfo.map(_.lastErrorMsg).getOrElse(""),
			lastError = errorInfo.map(_.lastError).getOrElse(""),
			lastErrorTime = errorInfo.map(_.lastErrorTime).getOrElse(-1L)
		)
	}
}
