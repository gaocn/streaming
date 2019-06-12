package org.apache.spark.streaming.receiver.input

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

/**
	* 接收器接收到数据按照block方式存储
	*/
sealed trait ReceivedBlock

/** 数组构成的一个block */
case class ArrayBufferBlock(arrayBuffer: ArrayBuffer[_]) extends ReceivedBlock

/** 迭代器构成的一个block */
case class IteratorBlock(iter:Iterator[_]) extends ReceivedBlock

/** 字节缓冲区构成的一个block */
case class ByteBufferBlock(byteBuf: ByteBuffer) extends ReceivedBlock
