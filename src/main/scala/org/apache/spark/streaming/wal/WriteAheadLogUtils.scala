package org.apache.spark.streaming.wal

import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkConf}

import scala.util.control.NonFatal

object WriteAheadLogUtils extends Logging{
	val RECEIVER_WAL_ENABLE_CONF_KEY = "spark.streaming.receiver.writeAheadLog.enable"
	val RECEIVER_WAL_CLASS_CONF_KEY = "spark.streaming.receiver.writeAheadLog.class"
	val RECEIVER_WAL_ROLLING_INTERVAL_CONF_KEY =
		"spark.streaming.receiver.writeAheadLog.rollingIntervalSecs"
	val RECEIVER_WAL_MAX_FAILURES_CONF_KEY = "spark.streaming.receiver.writeAheadLog.maxFailures"
	val RECEIVER_WAL_CLOSE_AFTER_WRITE_CONF_KEY =
		"spark.streaming.receiver.writeAheadLog.closeFileAfterWrite"

	val DRIVER_WAL_CLASS_CONF_KEY = "spark.streaming.driver.writeAheadLog.class"
	val DRIVER_WAL_ROLLING_INTERVAL_CONF_KEY =
		"spark.streaming.driver.writeAheadLog.rollingIntervalSecs"
	val DRIVER_WAL_MAX_FAILURES_CONF_KEY = "spark.streaming.driver.writeAheadLog.maxFailures"
	val DRIVER_WAL_BATCHING_CONF_KEY = "spark.streaming.driver.writeAheadLog.allowBatching"
	val DRIVER_WAL_BATCHING_TIMEOUT_CONF_KEY = "spark.streaming.driver.writeAheadLog.batchingTimeout"
	val DRIVER_WAL_CLOSE_AFTER_WRITE_CONF_KEY =
		"spark.streaming.driver.writeAheadLog.closeFileAfterWrite"

	val DEFAULT_ROLLING_INTERVAL_SECS = 60
	val DEFAULT_MAX_FAILURES = 3

	def enableReceiverLog(conf: SparkConf): Boolean = {
		conf.getBoolean(RECEIVER_WAL_ENABLE_CONF_KEY, false)
	}

	def getRollingIntervalSecs(conf: SparkConf, isDriver: Boolean): Int = {
		if (isDriver) {
			conf.getInt(DRIVER_WAL_ROLLING_INTERVAL_CONF_KEY, DEFAULT_ROLLING_INTERVAL_SECS)
		} else {
			conf.getInt(RECEIVER_WAL_ROLLING_INTERVAL_CONF_KEY, DEFAULT_ROLLING_INTERVAL_SECS)
		}
	}

	def getMaxFailures(conf: SparkConf, isDriver: Boolean): Int = {
		if (isDriver) {
			conf.getInt(DRIVER_WAL_MAX_FAILURES_CONF_KEY, DEFAULT_MAX_FAILURES)
		} else {
			conf.getInt(RECEIVER_WAL_MAX_FAILURES_CONF_KEY, DEFAULT_MAX_FAILURES)
		}
	}

	def isBatchingEnabled(conf: SparkConf, isDriver: Boolean): Boolean = {
		isDriver && conf.getBoolean(DRIVER_WAL_BATCHING_CONF_KEY, defaultValue = true)
	}

	/**
		* How long we will wait for the wrappedLog in the BatchedWriteAheadLog to write the records
		* before we fail the write attempt to unblock receivers.
		*/
	def getBatchingTimeout(conf: SparkConf): Long = {
		conf.getLong(DRIVER_WAL_BATCHING_TIMEOUT_CONF_KEY, defaultValue = 5000)
	}

	def shouldCloseFileAfterWrite(conf: SparkConf, isDriver: Boolean): Boolean = {
		if (isDriver) {
			conf.getBoolean(DRIVER_WAL_CLOSE_AFTER_WRITE_CONF_KEY, defaultValue = false)
		} else {
			conf.getBoolean(RECEIVER_WAL_CLOSE_AFTER_WRITE_CONF_KEY, defaultValue = false)
		}
	}

	/**
		* 为receiver创建一个WriteAheadLog实例，用户可配置类名：
		* 		spark.streaming.receiver.writeAheadLog.class
		* 自定义WriteAheadLog实例，默认创建FileBasedWriteAheadLog
		* @param sparkConf
		* @param fileWalLogDirectory
		* @param fileWalHadoopConf
		* @return
		*/
	def createLogForReceiver(
			sparkConf: SparkConf,
			fileWalLogDirectory: String,
			fileWalHadoopConf: Configuration): WriteAheadLog = {
		createLog(false, sparkConf, fileWalLogDirectory, fileWalHadoopConf)
	}

	def createLogForDriver(
			sparkConf: SparkConf,
			fileWalLogDirectory: String,
			fileWalHadoopConf: Configuration): WriteAheadLog = {
		createLog(true,sparkConf, fileWalLogDirectory, fileWalHadoopConf)
	}

	/**
		* 根据配置创建指定WriteAheadLog实例，通过SparkConf获取指定的WriteAheadLog
		* 类名，若用户指定，则通过调用
		* 1、`CustomLog(SparkConf, logDir)`实例化，若失败
		* 2、尝试`CustomLog(SparkConf)`实例化对象，若也失败则创建失败；
		* 若没有执行自定义的WriteAheadLog类名，则采用默认的实例。
		*/
	private def createLog(isDriver: Boolean,  sparkConf:SparkConf, fileWalLogDirectory:String, fileWalHadoopConf: Configuration): WriteAheadLog = {
		val classNameOption = if(isDriver) {
			sparkConf.getOption(DRIVER_WAL_CLASS_CONF_KEY)
		} else {
			sparkConf.getOption(RECEIVER_WAL_CLASS_CONF_KEY)
		}

		val wal = classNameOption.map{className=>
			try {
				instantiateClass(Utils.classForName(className).asInstanceOf[Class[_ <: WriteAheadLog]], sparkConf)
			} catch {
				case NonFatal(e) =>
					throw new Exception(s"无法创建${className}的WriteAheadLog实例")
			}
		}.getOrElse{
			new FileBasedWriteAheadLog(sparkConf, fileWalLogDirectory, fileWalHadoopConf,
				getRollingIntervalSecs(sparkConf, isDriver),
				getMaxFailures(sparkConf, isDriver),
				shouldCloseFileAfterWrite(sparkConf, isDriver))
		}

		if(isBatchingEnabled(sparkConf, isDriver)) {
			new BatchedWriteAheadLog(wal, sparkConf)
		} else {
			wal
		}
	}

	/** 初始化执行类实例 先尝试使用单个参数的构造器，若失败则采用无参构造器 */
	private def instantiateClass(cls: Class[_  <: WriteAheadLog], conf: SparkConf):WriteAheadLog = {
		try {
			cls.getConstructor(classOf[SparkConf]).newInstance(conf)
		} catch {
			case nsme:NoSuchMethodException =>
				cls.getConstructor().newInstance()
		}
	}
}
