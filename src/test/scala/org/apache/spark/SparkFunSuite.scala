package org.apache.spark

import org.scalatest.{FunSuite, Outcome}

abstract class SparkFunSuite extends FunSuite with Logging {

	/**
		* 打印测试开始，测试结束的日志标记
		*/
	final protected override def withFixture(test: NoArgTest): Outcome = {
		val testName = test.text
		val suiteName = this.getClass.getName
		val shortSuitName = suiteName.replaceAll("org.apache.spark","o.a.s")

		try {
			logInfo(s"\n\n===== TEST OUTPUT FOR ${shortSuitName}: '${testName}' =====\n")
			test()
		} finally{
			logInfo(s"\n\n===== FINISHED ${shortSuitName}: '${testName}' =====\n")
		}
	}
}
