package org.apache.spark.streaming

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

/**
	* 用于暂定或继续正在进行的
	*/
class ContextWaiter {
	private val lock = new ReentrantLock()
	private val condition = lock.newCondition()

	// 同步变量，由`lock`守护
	private var error:Throwable = null
	// 同步变量，由`lock`守护
	private var stopped: Boolean = false

	/**
		* 遇到错误，终止当前等待过程
		* @param e
		*/
	def notifyError(e: Throwable): Unit = {
		lock.lock()
		try {
			error = e
			//唤醒所有等待在当前condition实例上的线程
			condition.signalAll()
		} finally {
			lock.unlock()
		}

	}

	/**
		* 显示通知终止当前等待过程
		*/
	def notifyStop():Unit = {
		lock.lock()
		try {
			stopped = true
			//唤醒所有等待线程
			condition.signalAll()
		} finally {
			lock.unlock()
		}
	}

	/**
		* 挂起当前线程，挂起时间为timeout毫秒，-1表示一直挂起
		* @param timeout 挂起时长
		* @return true 表示正常停止，通过调用notifyStop暂停挂起状态
		*         否则出现异常，则将异常抛出
		*/
	def waitForStopOrError(timeout: Long = -1): Boolean = {
		lock.lock()
		try {
			if (timeout < 0) {
				while (!stopped && error == null) {
					//挂起当前线程，释放资源
					condition.await()
				}
			} else {
				var nanos = TimeUnit.MICROSECONDS.toNanos(timeout)
				while (!stopped && error == null && nanos> 0) {
					nanos = condition.awaitNanos(nanos)
				}
			}

			//若出现异常则抛出
			if (error != null) throw error

			//若正常超时退出
			stopped
		} finally {
			lock.unlock()
		}
	}
}
