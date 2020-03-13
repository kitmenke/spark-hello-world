package com.kitmenke.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite

class MyStreamingAppTests extends FunSuite with DataFrameSuiteBase {
  test("should parse json and sum all of the integers") {
    import sqlContext.implicits._

    // create some sample data to run through our program
    val input = sc.parallelize(
      List[(Int, String)](
        (1, "{ \"random-name\": \"Bob\", \"random-int\": 42 }"),
        (1, "{ \"random-name\": \"Mary\", \"random-int\": 100 }")
    )).toDF("key", "value")

    val expected = sc.parallelize(List(Some(142L))).toDF("s")
    val actual = MyStreamingApp.compute(input)
    actual.printSchema()
    actual.show()
    assertDataFrameEquals(actual, expected) // equal
  }
}
