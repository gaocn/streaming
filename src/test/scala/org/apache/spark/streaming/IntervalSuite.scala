package org.apache.spark.streaming

class IntervalSuite extends STestSuiteBase {

	test("less") {
		assert((new Interval(1, 7) < new Interval(8, 14)))
		assert(!(new Interval(999, 1100) < new Interval(999, 1100)))
	}

	test("lessEq") {
		assert(new Interval(999, 1100) <= new Interval(999, 1100))
		assert(!(new Interval(6, 12) <= new Interval(1, 7)))
	}

	test("greater") {
		assert(new Interval(999, 1100) > new Interval(899, 1000))
		assert(!(new Interval(3, 5) > new Interval(4, 6)))
	}

	test("greaterEq") {
		assert(new Interval(999, 1100) >= new Interval(999, 1100))
		assert(!(new Interval(3, 5) >= new Interval(4, 6)))
	}

	test("currentInterval") {
		logInfo(Interval.currentInterval(new Duration(100)).toString)
	}

}
