package org.apache.spark.streaming.scheduler

import org.apache.spark.streaming.Time
import org.apache.spark.streaming.domain.OutputOperationInfo
import org.apache.spark.util.{CallSite, Utils}

import scala.util.{Failure, Try}

/**
	* 代表Spark的计算操作，其中可能包含多个Spark作业
	*/
private[streaming]
class Job(val time: Time, func:()=> _) {
	private var _id: String = _
	private var _outputOpId: Int = _
	private var isSet = false
	private var _result:Try[_] = _
	private var _callSite:CallSite = null
	private var _startTime:Option[Long] = None
	private var _endTime:Option[Long] = None


	def run(): Unit = {
		_result = Try(func())
	}

	def result: Try[_] = {
		if(_result == null) {
			throw new IllegalStateException("作业尚未完成，无法获取结果。")
		}
		_result
	}

	/**
		* 全局唯一的Job Id标识
		*/
	def id:String = {
		if(!isSet) {
			throw new IllegalStateException("作业Id未设置。")
		}
		_id
	}

	/**
		* JobSet中的每个Job的唯一 output op id标识
		*/
	def outputOpId: Int = {
		if(!isSet) {
			throw new IllegalStateException("outoutOpId未设置。")
		}
		_outputOpId
	}

	def setOutputOpId(outputOpId:Int): Unit = {
		if(isSet) {
			throw new IllegalStateException("outoutOpId已设置，无法重新设置。")
		}
		isSet = true
		_id = s"streaming job $time.$outputOpId"
		_outputOpId = outputOpId
	}

	def setCallSite(callSite: CallSite): Unit = {
		_callSite = callSite
	}

	def callSite: CallSite = _callSite

	def setStartTime(startTime: Long): Unit = {
		_startTime = Some(startTime)
	}

	def setEndTime(endTime: Long): Unit = {
		_endTime = Some(endTime)
	}

	def toOutputOperationInfo: OutputOperationInfo = {

		val failureReason = if(_result != null && _result.isFailure) {
			Some(Utils.exceptionString(_result.asInstanceOf[Failure[_]].exception))
		} else {
			None
		}
		OutputOperationInfo(
			time, outputOpId, callSite.shortForm, callSite.longForm, _startTime, _endTime, failureReason)
	}

	override def toString: String = id
}
