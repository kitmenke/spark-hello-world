package com.kitmenke.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql
import org.apache.spark.sql.Row
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
    result.show(truncate = false)
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

  test("should drop duplicates") {
    val schema = new StructType()
      .add("row", StringType, nullable = false)
      .add("code", StringType, nullable = false)
    // create some sample data to run through our program
    val rdd = sc.parallelize(Seq(
      Row("row1", "XFH"),
      Row("row2", "ABC"),
      Row("row3", "XFH"),
    ))
    val df = sqlContext.createDataFrame(rdd, schema)
    df.show()
    val result = df.dropDuplicates("code")
    result.printSchema()
    result.show()
  }

  test("should join two dataframes") {
    val schema1 = new StructType()
      .add("ColumnA", StringType, nullable = false)
      .add("ColumnB", StringType, nullable = false)
    // create some sample data to run through our program
    val rdd1 = sc.parallelize(Seq(
      Row("a1", "b1"),
      Row("a2", "b2"),
      Row("a3", "b3"),
    ))
    val df1 = sqlContext.createDataFrame(rdd1, schema1)
    val schema2 = new StructType()
      .add("ColumnA", StringType, nullable = false)
      .add("ColumnC", StringType, nullable = false)
    val rdd2 = sc.parallelize(Seq(
      Row("a1", "c1"),
      Row("a2", "c2"),
      Row("a4", "c3"),
    ))
    val df2 = sqlContext.createDataFrame(rdd2, schema2)

    val result = df1.join(df2, df1("ColumnA") === df2("ColumnA"), "outer")
    result.printSchema()
    result.show()
  }
}
