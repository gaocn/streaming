package org.apache.spark.streaming

import java.io._
import java.util.concurrent.{Executors, RejectedExecutionException, TimeUnit}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.io.CompressionCodec
import org.apache.spark.streaming.scheduler.JobGenerator
import org.apache.spark.util.MetadataCleaner
import org.apache.spark.{Logging, SparkConf}

import scala.tools.nsc.interpreter.InputStream

class Checkpoint(ssc: SContext, val checkpointTime: Time) extends Logging with Serializable {
	val master = ssc.sc.master
	val framework = ssc.sc.appName
	val jars  = ssc.sc.jars
	val graph = ssc.graph
	val checkpointDir = ssc.checkpointDir
	val checkpointDuration:Duration = ssc.checkpointDuration
	val pendingTimes = ssc.scheduler.getPendingTimes().toArray
	val delaySeconds = MetadataCleaner.getDelaySeconds(ssc.conf)
	val sparkConfPairs = ssc.conf.getAll

	/**
		* 根据持久化的内存创建SparkConf实例
		*/
	def createSparkConf(): SparkConf = {
		/** 从保存checkpoint恢复这些配置*/
		val propertiesToReload = List(
			"spark.driver.host",
			"spark.driver.port",
			"spark.master",
			"spark.yarn.keytab",
			"spark.yarn.principal",
			"spark.yarn.app.id",
			"spark.yarn.app.attemptid",
			"spark.ui.filter"
		)

		//根据保存的状态创建而不是采用默认配置项
		val newSparkConf = new SparkConf(loadDefaults = false)
			.setAll(sparkConfPairs)
  		.remove("spark.diver.host")
  		.remove("spark.diver.port")

		val newReloadConf = new SparkConf(loadDefaults = true)
		propertiesToReload.foreach{prop =>
			newReloadConf.getOption(prop).foreach{value=>
				newSparkConf.set(prop, value)
			}
		}

		//TODO Add Yarn proxy filter specific configurations to the recovered SparkConf
		val filter = "org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter"
		val filterPrefix = s"spark.$filter.param."
		newReloadConf.getAll.foreach{case (k, v) =>
			if(k.startsWith(filterPrefix) && k.length > filterPrefix.length) {
				newSparkConf.set(k, v)
			}
		}

		newSparkConf
	}

	def  validate(): Unit ={
		assert(master != null, "Checkpoint.master的值不能为空")
		assert(framework != null, "Checkpoint.framework（流应用程序名称）的值不能为空")
		assert(graph != null, "Checkpoing.grapg的值不能为空")
		assert(checkpointTime != null, "Checkpoint.checkpointTime的值不能为空")
		logInfo(s"验证${checkpointTime}时刻的Checkpoint通过！")
	}
}

private[streaming] object Checkpoint extends Logging {
	val PREFIX = "checkpoint_"
	val SUFFIX = ".bk"
	val REGEX = (PREFIX + """([\d]+)([\w\.]*)""").r

	/**
		* 根据checkpoint时间和目录创建checkpoing文件
		* @param checkpointDir HDFS目录
		* @param checkpointTime checkpoint时间
		* @return HDFS Path
		*/
	def checkpointFile(checkpointDir: String,  checkpointTime: Time):Path = {
		new Path(checkpointDir, PREFIX + checkpointTime.milliseconds)
	}

	/**
		* 创建指定checkpoin目录和时间的备份文件
		* @param checkpointDir HDFS目录
		* @param checkpointTime checkpoint时间
		* @return HDFS Path
		*/
	def checkpointBackupFile(checkpointDir: String, checkpointTime: Time):Path = {
		new Path(checkpointDir, PREFIX + checkpointTime.milliseconds + SUFFIX)
	}

	/**
		* 按照文件创建时间递增返回指定checkpoint目录下的所有文件
		* @param checkpointDir checkpoint文件存放目录
		* @param fsOption 可选，HDFS FileSystem
		* @return
		*/
	def getChechkpointFiles(
				 checkpointDir: String,
				 fsOption: Option[FileSystem] = None): Seq[Path] = {
		def sortPath(p1: Path,  p2: Path):Boolean = {
			val (time1, bk1) = p1.getName match {case REGEX(x, y) => (x.toLong, !y.isEmpty)}
			val (time2, bk2) = p2.getName match {case REGEX(x, y) => (x.toLong, !y.isEmpty)}
			(time1 < time2) || (time1 == time2 && bk1)
		}


		val checkpointPath = new Path(checkpointDir)
		val fs = fsOption.getOrElse(checkpointPath.getFileSystem(SparkHadoopUtil.get.conf))

		if (fs.exists(checkpointPath)) {
			val statuses = fs.listStatus(checkpointPath)
			if (statuses != null) {
				val filtered = statuses
					.map(_.getPath)
					.filter(p => REGEX.findFirstMatchIn(p.toString).nonEmpty)
				filtered.sortWith(sortPath).toSeq
			} else {
				logWarning(s"checkpoint目录[${checkpointDir}]的FileStats为空，返回空列表")
				Seq.empty
			}
		} else {
			logWarning(s"checkpoint目录[${checkpointDir}]为空，返回空列表")
			Seq.empty
		}
	}

	/**
		* 序列化Checkpoint实例为字节数组，抛出任何序列化过程中出现的错误
		* @param checkpoint 要实例化的对象
		* @param conf SparkConf
		* @return 字节数组
		*/
	def serialize(checkpoint: Checkpoint, conf: SparkConf):Array[Byte] = {
		val compressionCodec = CompressionCodec.createCodec(conf)
		val bos = new ByteArrayOutputStream()
		val zos = compressionCodec.compressedOutputStream(bos)
		val oos = new ObjectOutputStream(zos)

		try {
			oos.writeObject(checkpoint)
		}catch{
			case e: Exception  =>
				logWarning("序列化checkpoint对象过程中出错！")
				throw new Exception(e)
		} finally {
			if(oos != null) {
				oos.close()
			}
		}
		bos.toByteArray
	}

	/**
		* 反序列化输入流为Checkpoint实例，抛出序列化过程中出现的错误
		* @param inputStream
		* @param conf
		* @return
		*/
	def deserialize(inputStream: InputStream, conf: SparkConf): Checkpoint = {
		val compressionCodec = CompressionCodec.createCodec(conf)
		val zis = compressionCodec.compressedInputStream(inputStream)
		var ois:ObjectInputStreamWithLoader = null
		var cp:Checkpoint = null
		try {
			/**
				* ObjectInputStream会采用在栈中最近的一个user-defined的
				* 类加载器寻找类实例，可能导致无法找到类实例而报ClassNotFound
				* 异常因此这里显示采用当前线程所在的类加载器来寻找类实例。
				* 这是个已知的Java问题，具体参见：
				* 	http://jira.codehaus.org/browse/GROOVY-1627
				*/
			ois = new ObjectInputStreamWithLoader(zis, Thread.currentThread().getContextClassLoader)
			cp = ois.readObject().asInstanceOf[Checkpoint]
			cp.validate()
		} catch {
			case e: Exception =>
				logWarning("")
				throw new Exception(e)
		} finally {
			if (ois != null) {
				ois.close()
			}
		}
		cp
	}
}

/**
	* 指定类加载器，解决ObjectInputStream寻找类时使用的类加载不恰当导
	* 致ClassNotFound异常。
	*/
private[streaming] class ObjectInputStreamWithLoader(
														in:InputStream,
														loader: ClassLoader
													) extends ObjectInputStream(in) with Logging {
	override def resolveClass(desc: ObjectStreamClass): Class[_] = {
			try {
				Class.forName(desc.getName, false, loader)
			} catch {
				case e: Exception =>
					//忽略异常
				logWarning(s"解析类${desc.getName}异常：${e.getMessage}")
			}
		super.resolveClass(desc)
	}

}

/**
	* 工具类方法，用于实现将graph的checkpoint到文件中。
	* 每个CheckpointWriter实例内部采用一个工作线程的线程池进行checkpoint
	* 操作，该实例是线程安全的。
	*/
private[streaming] class CheckpointWriter(
														jobGenerator: JobGenerator,
														checkpointDir: String,
														conf: SparkConf,
														hadoopConf: Configuration
												 ) extends Logging {
	val MAX_ATTEMPTS = 3
	val executor = Executors.newFixedThreadPool(1)
	val compressionCodec = CompressionCodec.createCodec(conf)
	private var stopped = false
	private var fs_ : FileSystem = _
	/** 上一次进行checkpoint的时间 */
	@volatile private var lastCheckpointTime: Time = null

	private def fs = synchronized{
		if (fs_ == null) {
			fs_ = new Path(checkpointDir).getFileSystem(hadoopConf)
		}
		fs_
	}

	private def resetFs() = synchronized{
		fs_ = null
	}

	/**
		* 1、先将checkpoint序列化为字节数组；
		* 2、启动线程池中工作线程对序列化后的字节数组进行持久化，CheckpointWriteHandler
		* 的主要步骤：
		* 	(1) 确定lastCheckpointTime，对滞后的Batch仍然采用上一次
		* 		checkpoint的时间；
		*   (2) 创建临时文件temp，若存在现将其删除，将序列化字节数组写入
		*   	临时文件；
		*   (3) 判断是否存在lastCheckpointTime时刻同名的文件，若有将其
		*   	重名为.bk后缀的文件。若.bk文件也存在则先将其删除后再重命名。
		*   (4) 将临时文件temp重命名为最终lastCheckpointTime时刻的文
		*   	件名。
		*   (5) 删除旧的checkpoint文件，只保留最近的10个文件
		*   (6) 调用JobGenerator回调函数，通知checkpoint操作成功
		*/
	def write(checkpoint: Checkpoint, clearCheckpointDataLater: Boolean): Unit ={
		try {
			val bytes = Checkpoint.serialize(checkpoint, conf)
			executor.submit(new CheckpointWriteHandler(checkpoint.checkpointTime, bytes, clearCheckpointDataLater))
			logInfo(s"提交checkpoint[time=${checkpoint.checkpointTime}]到队列，准备进行持久化。")
		} catch {
			case rej: RejectedExecutionException =>
				logError(s"线程池拒绝执行队列中checkpoint[time=${checkpoint.checkpointTime}]的任务，原因：${rej.getMessage}。")
		}
	}

	/** 停止checkpoint过程 */
	def stop(): Unit = synchronized{
		if (stopped) return
		//1. 停止接收任务，等待工作线程执行完毕
		executor.shutdown()

		val startTime =  System.currentTimeMillis()

		//2. 等待线程池10s，若任务仍然没有结束，则停止等待强制关闭线程池
		val terminated = executor.awaitTermination(10, TimeUnit.SECONDS)
		if (!terminated)  {
			executor.shutdownNow()
		}

		val endTime = System.currentTimeMillis()
		logInfo(s"CheckpointWriter线程池停止，停止线程池耗时: ${endTime - startTime} ms")
		stopped = true
	}


	/**
		* Runnable实例，用于对序列化后的数据进行persist操作。
		* @param checkpointTime 当前进行checkpoint的时间
		* @param bytes 序列化后的字节数组
		* @param clearCheckpointDataLater 是否会清理该数据
		*/
	class CheckpointWriteHandler(
					checkpointTime: Time,
					bytes: Array[Byte],
					clearCheckpointDataLater: Boolean
				) extends Runnable {
		override def run(): Unit = {
			if (lastCheckpointTime == null || lastCheckpointTime < checkpointTime) {
				lastCheckpointTime = checkpointTime
			}

			var attempts = 0
			val startTime = System.currentTimeMillis()
			val tempFile = new Path(checkpointDir, "temp")

			/**
				* 在每次generating和completing a batch时，进行checkpoint，
				*
				* 当一个batch的处理时间大于Batch Interval时，某个旧的batch
				* 的checkpoint操作可能在一个新的batch的checkpoint之后，此
				* 时仍然会进行checkpoint，但是会用最新的lastCheckpointTime
				* 对其进行标记而不是采用旧batch的checkpointTime。在恢复时
				* 将其最新的checkpoint信息来恢复！！
				*
				* 当前写操作只有一个线程进行，因此是线程安全的。
				*/
			val checkpointFile = Checkpoint.checkpointFile(checkpointDir, lastCheckpointTime)
			val backupCpFile = Checkpoint.checkpointBackupFile(checkpointDir, lastCheckpointTime)

			while (attempts < MAX_ATTEMPTS && !stopped) {
				attempts += 1
				try {
					logInfo(s"开始将[time=${checkpointTime}]时刻的checkpoint对象持久化到文件${checkpointFile}中")

					//1. 将数据写入临时文件
					if(fs.exists(tempFile)) {
						fs.delete(tempFile, true)
					}

					val fos = fs.create(tempFile)
					try {
						fos.write(bytes)
					}finally {
						fos.close()
					}

					//2. 若checkpointFile存在在将其备份
					if(fs.exists(checkpointFile)) {
						if (fs.exists(backupCpFile))  {
							fs.delete(backupCpFile, true)
						}
						if (!fs.rename(checkpointFile, backupCpFile)) {
							logWarning(s"将${checkpointFile}重命名为${backupCpFile}失败")
						}
					}

					//3. 将临时文件重名为最终的checkpoint文件
					if(!fs.rename(tempFile, checkpointFile)) {
						logWarning("将临时文件${checkpointFile}重命名为${backupCpFile}失败")
					}

					//4. 删除old checkpoint文件
					val allCheckpointFiles = Checkpoint.getChechkpointFiles(checkpointDir, Some(fs))
					if(allCheckpointFiles.size > 10) {
						allCheckpointFiles.take(allCheckpointFiles.size - 10).foreach{file  =>
							logInfo(s"删除旧的checkpoint文件：${file}")
							fs.delete(file, true)
						}
					}

					val finishTime =  System.currentTimeMillis()
					logInfo(s"成功将[time=${checkpointTime}]时刻的checkpoint对象持久化到文件${checkpointFile}中，耗时：${finishTime  - startTime} ms")
					jobGenerator.onCheckpointCompletion(checkpointTime, clearCheckpointDataLater)
					return
				} catch {
					case ioe: IOException =>
						logWarning(s"在第${attempts}尝试将checkpoint对象持久化到文件${checkpointFile}中时出现异常：${ioe.getMessage}")
						resetFs()
				}
			}
			logWarning(s"无法将[time=${checkpointTime}]时刻的checkpoint对象持久化到文件${checkpointFile}中")
		}
	}
}

/**
	* 从指定checkpoint文件夹下读取最近一次的checkpoin文件并反序列化为
	* Checkpoint实例。
	*/
private[streaming] object CheckpointReader extends Logging {

	/**
		* 从指定checkpointDir目录恢复恢复最近一次的checkpoint状态，若
		* 没有持久化的状态，则返回None。
		*/
	def read(checkpointDir: String):Option[Checkpoint] = {
		read(checkpointDir, new SparkConf(), SparkHadoopUtil.get.conf, ignoreReadError = true)
	}

	/**
		* 从指定checkpointDir目录恢复恢复最近一次的checkpoint状态，若
		* 没有持久化的状态，则返回None。
		* @param checkpointDir checkpoint文件所在目录
		* @param conf          SparkConf
		* @param hadoopConf    Configuration
		* @param ignoreReadError true恢复状态过程中若出错，则抛出
		*                        false恢复状态过程中若出错，则不会抛
		*                        出直接返回None
		* @return Option[Checkpoint]
		*/
	def read(
						checkpointDir:String,
						conf:SparkConf,
						hadoopConf: Configuration,
						ignoreReadError:Boolean = false):Option[Checkpoint] = {

		val checkpointPath = new Path(checkpointDir)
		//TODO(rein) 为什么要使用def而不是val？！
		def fs:FileSystem = checkpointPath.getFileSystem(hadoopConf)

		//所有的checkpoint文件，按照最近创建时间排序
		val checkpointFiles =  Checkpoint.getChechkpointFiles(checkpointDir, Some(fs)).reverse
		if(checkpointFiles.isEmpty) {
			return None
		}

		logInfo(s"发现checkpoint文件(按照最近创建时间排序)：${checkpointFiles.mkString("\\n")}")

		var readError:Exception = null
		checkpointFiles.foreach{file =>
			try {
				val inputStream = fs.open(file)
				val cp = Checkpoint.deserialize(inputStream, conf)
				logInfo("成功从文checkpoint件读取并加载内容")
				logInfo(s"恢复时采用的状态是: ${cp.checkpointTime}来自时刻的checkpoint状态")
				return Some(cp)
			}catch {
				case e: Exception =>
					readError = e
					logWarning(s"读取checkpoint文件[${file}]进行反序列化过程中出错: ${e.getMessage}")
			}
		}

		if (!ignoreReadError) {
			throw new  Exception(s"从${checkpointDir}尝试恢复状态时失败，失败原因：${readError.getMessage}")
		}
		None
	}
}