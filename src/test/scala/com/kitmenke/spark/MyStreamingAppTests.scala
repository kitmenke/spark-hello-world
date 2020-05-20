package com.kitmenke.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
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
    result.show()
  }

  test("should parse dates") {
    // create some sample data to run through our program
    val df = sc.parallelize(List[String]("20200401", "20200501", "20200601"))
      .toDF("dates")
    // using the to_date spark sql function, convert the string value into a
    import org.apache.spark.sql.functions.to_date
    val result = df.select(to_date(df("dates"), "yyyyMMdd"))
    result.printSchema()
    result.show()
  }
}
