package org.apache.spark.streaming.receiver

import java.nio.ByteBuffer

import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
	* 运行在work node上接收器，用于接收外部数据。
	*
	* 自定义接收器可以通过重写Receiver的`onStart`和`onStop`方法实现，
	* 	(1) `onStart`方法包括启动接收数据的步骤；
	*   (2) `onStop`方法包括停止接收数据的步骤；
	*	若接收数据过程中遇到异常可以用`restart`重启接收器，也可以用`stop`
	*	停止接收器。
	*
	*	自定义接收器的举例：
	*
	*	class MyReceiver(storageLevel:StorageLevel) extends Receiver[String](storageLevel) {
	*	    def onStart() {
	*				// 启动接收器，例如：启动线程，打开socket连接等...
	*			  // ** 必须新创建线程接收数据，因为onStart不能是阻塞的！！
	*
	*			  // 在线程内部调用store()方法将接收数据存放内存
	*
	*
	*			  // 当接收线程发生异常时调用stop()、restart()或reportError()处理异常
	*
	*	    }
	*
	*
	*	    def onStop() {
	*	       // 关闭接收器，如：停止线程、关闭socket连接等...
	*	    }
	*	}
	*/
abstract class Receiver[T](val storageLevel: StorageLevel) extends Serializable {
	/**
		* 当启动接收器时需要调用该方法，该方法内部需要初始化所有需要使用的
		* 资源(线程，缓冲区等)以便能够接收数据。
		*
		* 该方法不能是阻塞的，因此接收数据必须另起一个线程，接收的数据通过
		* 调用`store`保存数据到内存或磁盘。
		*
		* 若线程中出现异常或错误，可以采用如下方法：
		* 1、调用`reportError(...)`方法向Driver报错，而接收数据过程仍
		* 然继续。
		* 2、调用`stop(...)`方法停止接收器，该方法会调用`onStop`方法释
		* 放`onStart`中创建的资源。
		* 3、调用`restart(...)`方法重启接收器，该方法会首先调用`onStop`
		* 释放资源，延迟一段时间(spark.streaming.receiverRestartDelay=2000)
		* 后调用`onStart`方法启动接收器
		*
		*/
	def onStart(): Unit = {}

	/**
		* 停止接收器时需要调用的方法，用于释放线程、连接等资源
		*/
	def onStop():Unit = {}

	/*
	 *=========================
	 * 接收数据存储方法
	 *=========================
	 */

	/**
		* 将该条数据存储到Spark缓存中，多条数据会被聚合到一个block中后，
		* 才会被push到Spark的内存中。
		*/
	def store(dataItem: T): Unit = {
		supervisor.pushSingle(dataItem)
	}

	/**
		* 将数组中的数据作为一个Block被push到Spark的内存中。
		* metadata元数据会与该block数据关联以便在InputDStream中被使用。
		*/
	def store(arrBuffer: ArrayBuffer[T]): Unit = {
		supervisor.pushArrayBuffer(arrBuffer, None, None)
	}

	def store(arrayBuffer: ArrayBuffer[T], metadata: Any): Unit = {
		supervisor.pushArrayBuffer(arrayBuffer, Some(metadata), None)
	}

	/**
		* 将迭代器中的数据作为一个Block被push到Spark的内存中。
		* metadata元数据会与该block数据关联以便在InputDStream中被使用。
		*/
	def store(iter: Iterator[T]): Unit = {
		supervisor.pushIterator(iter, None, None)
	}

	def store(iter: Iterator[T], metadata: Any): Unit = {
		supervisor.pushIterator(iter, Some(metadata), None)
	}

	/**
		* 将字节流作为一个block被push到Spark的内存中。
		* metadata元数据会与该block数据关联以便在InputDStream中被使用。
		*
		* PS：字节流中序列化后数据需要与Spark中配置的序列化器一致
		*/
	def store(byteBuf: ByteBuffer): Unit = {
		supervisor.pushBytes(byteBuf, None, None)
	}

	def store(byteBuf: ByteBuffer, metadata: Any): Unit = {
		supervisor.pushBytes(byteBuf, Some(metadata), None)
	}

	/*
	 *=========================
	 * 接收线程遇到错误时的处理方法
	 *=========================
	 */

	/** 向Driver报告接收数据过程中出现的异常 */
	def reportError(msg:String, error: Throwable): Unit = {
		supervisor.reportError(msg, error)
	}

	/** 停止接收器 */
	def stop(msg: String): Unit = {
		supervisor.stop(msg, None)
	}

	def stop(msg:String, cause: Option[Throwable]): Unit = {
		supervisor.stop(msg, cause)
	}

	/**
		* 重启接收器，调用后立即返回，底层通过另起线程异步方式重启接收器。
		* 可以通过spark.streaming.receiverRestartDelay指定延迟启动时
		* 长。
		*/
	def restart(msg: String): Unit = {
		supervisor.restartReceiver(msg)
	}

	def restart(msg:String, cause: Option[Throwable]): Unit = {
		supervisor.restartReceiver(msg, cause)
	}

	def restart(msg:String, cause: Option[Throwable], delay: Int): Unit = {
		supervisor.restartReceiver(msg, cause, delay)
	}


	/** 判断是否接收器启动 */
	def isStarted: Boolean = {
		supervisor.isReceiverStarted
	}

	/** 判断接收器是否停止，通常用于是否停止接收数据 */
	def isStopped: Boolean = {
		supervisor.isReceiverStopped
	}

	/** 指定接收器所在的机器hostname，子类可以重写该方法 */
	def preferredLocation: Option[String] =  None

	/** 获取当前接收器关联的InputDStream */
	def streamId: Int = id
	/*
	* =================
	* Private methods
	* =================
	*/
	/** 用于关联当前接收器关联的InputDStream */
	private var id: Int = -1

	/** 设置接收器关联的InputDStream id*/
	private[streaming] def setReceiverId(_id: Int): Unit = {
		id = _id
	}


	/** 用于管理接收器的handler object，在worker上懒加载 */
	@transient
	private[streaming]  var  _supervisor:ReceiverSupervisor = null

	/** 将接收管理器与接收器关联 */
	private[streaming] def attachSupervisor(exec: ReceiverSupervisor) = {
		assert(_supervisor == null, "不允许重复关联")
		_supervisor = exec
	}

	/** 获取关联的管理器 */
	private[streaming] def supervisor:ReceiverSupervisor = {
		assert(_supervisor != null, "")
		_supervisor
	}
}
