package com.kitmenke.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite


class MyStreamingAppTests extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._

  test("should parse json") {
    // create some sample data to run through our program
    val input = sc.parallelize(
      List[String](
        "{\"name\":\"Jean-Luc Picard\",\"birth_year\": 2305}",
        "{\"name\":\"William Riker\",\"birth_year\": 2335}",
        "{\"name\":\"Deanna Troi\",\"birth_year\": 2336}"
      )).toDF("value")
    input.printSchema()
    input.show()
    // define our JSONs schema
    val schema = new StructType()
      .add("name", StringType, nullable = true)
      .add("birth_year", IntegerType, nullable = true)

    val result = input.select(sql.functions.from_json(input("value"), schema))
    result.printSchema()
    result.show(truncate = false)
  }

  test("compute method should convert dataframe") {
    // create some sample data to run through our program
    // TODO: create sample data

    // define our JSONs schema
    // TODO: define schema

    // transform the dataframe
    //val result = MyStreamingApp.compute(df)

    // TODO: validate result
  }
}
