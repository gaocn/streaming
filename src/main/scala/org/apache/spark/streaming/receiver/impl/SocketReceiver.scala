package org.apache.spark.streaming.receiver.impl

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.{ConnectException, Socket}

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.util.NextIterator

import scala.reflect.ClassTag
import scala.util.control.NonFatal

class SocketReceiver[T:ClassTag](
		host:String,
		port:Int,
		byteToObjects:InputStream => Iterator[T],
		storageLevel: StorageLevel) extends Receiver[T](storageLevel) with Logging{

	override def onStart(): Unit = {
		new Thread("Socket Receiver") {
			setDaemon(true)

			override def run(): Unit = {receive()}
		}.start()
	}

	override def onStop(): Unit = {
		//不需要做任何事，因为receive()在出错或结束时会自动停止
	}

	/**
		* 创建一个sokcet连接并接收数据，直到receiver停止
		*/
	private def receive(): Unit = {
		var socket:Socket = null
		try{
			logInfo(s"开始连接 ${host}:${port}")
			socket = new Socket(host, port)
			logInfo(s"socket连接建立成功：${socket}")

			val iterator = byteToObjects(socket.getInputStream)
			while(!isStopped && iterator.hasNext) {
				store(iterator.next())
			}
			if(!isStopped) {
				restart("Socket Data Stream中已没有数据")
			} else {
				logInfo("停止接收")
			}
		} catch {
			case e: ConnectException =>
				restart("无法连接 ${host}:${port}", Some(e))
			case NonFatal(e) =>
				logWarning(s"接收数据过程中发生错误：${e.getMessage}")
				restart("接收数据过程中发生错误")
		} finally {
			socket.close()
			logInfo("关闭连接 ${host}:${port}")
		}
	}
}


object SocketReceiver {
	/**
		* 从网络流中读取一行数据并解析为字符串的迭代器，默认网络流按照换行
		* 符"\n"分隔。
		*/
	def bytesToLines(inputStream:InputStream):Iterator[String] = {
		val dataIn = new BufferedReader(new InputStreamReader(inputStream))
		new NextIterator[String] {
			override protected def getNext(): String = {
				val nextValue = dataIn.readLine()
				if(nextValue == null)  {
					finished = true
				}
				nextValue
			}
			override protected def close(): Unit = {
				dataIn.close()
			}
		}
	}
}