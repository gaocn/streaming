package org.apache.spark.streaming

class ContextWaiterSuite extends STestSuiteBase {

	test("stoWaiter") {
		val cw  = new ContextWaiter()

		new Thread(){
			override def run(): Unit = {
				Thread.sleep(5000)
				cw.notifyStop()
				println("停止等待过程")
			}
		}.start()

		val isStopped = cw.waitForStopOrError()

		assert(isStopped, "可以正常停止")
	}

	test("errorWaiter") {
		val cw = new ContextWaiter

		new Thread(){
			override def run(): Unit = {
				Thread.sleep(5000)
				cw.notifyError(new Exception("发生异常，终止等待"))
			}
		}.start()

		val isStopped = cw.waitForStopOrError()

		assert(isStopped, "应该无法到达这里！！")
	}

}
