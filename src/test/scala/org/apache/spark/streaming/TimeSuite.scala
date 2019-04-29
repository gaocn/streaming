package org.apache.spark.streaming

class TimeSuite extends STestSuiteBase {
	test("less") {
		assert(new Time(999) < new Time(1000))
		assert(new Time(0) < new Time(1))
		assert(!(new Time(1000) < new Time(999)))
		assert(!(new Time(1000) < new Time(1000)))
	}

	test("lessEq"){
		assert(new Time(999) <= new Time(1000))
		assert(new Time(0) <= new Time(1))
		assert(!(new Time(1000) <= new Time(999)))
		assert((new Time(1000) <= new Time(1000)))
	}

	test("greater"){
		assert(new Time(9) > new Time(8))
		assert(new Time(2) > new Time(0))
		assert(!(new Time(1000) > new Time(1001)))
		assert(!(new Time(1000) > new Time(1000)))
	}

	test("greaterEq"){
		assert(new Time(9) >= new Time(8))
		assert(new Time(2) >= new Time(0))
		assert(!(new Time(1000) >= new Time(1001)))
		assert((new Time(1000) >= new Time(1000)))
	}

	test("plus") {
		assert((new Time(1000) + new Duration(10)) == new Time(1010))
		assert((new Time(1000) + new Duration(0)) == new Time(1000))
	}

	test("minus") {
		assert((new Time(1000) - new Duration(10)) == new Time(990))
		assert((new Time(1000) - new Duration(0)) == new Time(1000))
		assert((new Time(1000) - new Time(990)) == new Duration(10))
	}

	test("max") {
		assert((new Time(9) max new Time(1)) == new Time(9))
		assert((new Time(9) max new Time(9)) == new Time(9))
		assert((new Time(1) max new Time(0)) == new Time(1))
	}

	test("min") {
		assert((new Time(9) min new Time(1)) == new Time(1))
		assert((new Time(9) min new Time(9)) == new Time(9))
		assert((new Time(1) min new Time(0)) == new Time(0))
	}

	test("isMultipleOf") {
		assert((new Time(9) isMultipleOf  new Duration(1)) )
		assert(!(new Time(8) isMultipleOf new Duration(3)))
		assert((new Time(1) isMultipleOf new Duration(0)))
	}

	test("floor") {
		assert((new Time(9) floor  new Duration(1)) == new Time(9))
		assert((new Time(13) floor  new Duration(2)) == new Time(12))
		assert((new Time(15) floor  new Duration(7)) == new Time(14))
		assert((new Time(100) floor  (new Duration(7))) == new Time(98))
		assert((new Time(100) floor  (new Duration(7), new Time(10))) == new Time(94))
	}

	test("until"){
		assert((new Time(1000) until (new Time(1100), new Duration(100))) == Seq(new Time(1000)))
		assert((new Time(1000) until (new Time(1000), new Duration(100))) == Seq())
		assert((new Time(100) until (new Time(250), new Duration(30))) == Seq(new Time(100),new Time(130),new Time(160),new Time(190),new Time(220)))
	}

	test("to") {
		assert((new Time(1000) to (new Time(1100), new Duration(100))) == Seq(new Time(1000),new Time(1100)))
		assert((new Time(1000) to (new Time(1000), new Duration(100))) == Seq(new Time(1000)))
		assert((new Time(100) to (new Time(250), new Duration(30))) == Seq(new Time(100),new Time(130),new Time(160),new Time(190),new Time(220),new Time(250)))
	}


}
