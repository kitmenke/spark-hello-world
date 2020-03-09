package com.kitmenke.spark

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSpec
import org.apache.spark.sql
import org.apache.spark.sql.{SQLContext, SparkSession}

case class TestIntConversion(javaInteger: java.lang.Integer, scalaInt: scala.Int, scalaOptionalInt: Option[scala.Int])

class MyAppTests extends FunSpec with SharedSparkContext {
  describe("word count") {
    it("can read json") {
      val sqlContext = new SQLContext(sc)
      val events_sep22 = sqlContext.read.json("src/test/resources/data.json")
      events_sep22.select("data").printSchema()
      events_sep22.select("data.*").printSchema()
    }

    /*it("should be able to split a sentence into words") {
      // given
      val sentence = "one two three"
      val expected = Array("one", "two", "three")
      // when
      val actual = MyApp.splitSentenceIntoWords(sentence)
      // then
      assert(actual === expected)
    }

    it("should make words lowercase and remove punctuation") {
      // given
      val sentence = "Boy, that escalated QUICKLY..."
      val expected = Array("boy", "that", "escalated", "quickly")
      // when
      val actual = MyApp.splitSentenceIntoWords(sentence)
      // then
      assert(actual === expected)
    }
*/
    /*val schema = sql.types.StructType(List(
      sql.types.StructField("id", sql.types.DoubleType, nullable = false),
      sql.types.StructField("name", sql.types.StringType, nullable = true),
      sql.types.StructField("is_active", sql.types.BooleanType, nullable = true)
    ))
    val rows = Seq(
      sql.Row(1d, "Alice", true),
      sql.Row(1d, "Bob", false),
      sql.Row(1d, "Charlie", false)
    )
*/




    /*it("should be able to build a dataframe") {
      val sqlContext = new SQLContext(sc)
      val rows = Seq(
        TestIntConversion(1, 1, Some(1)),
        TestIntConversion(2, 2, None),
        TestIntConversion(null, 3, None)
      )
      val df = sqlContext.createDataFrame(rows)
      df.printSchema()
      df.show()
    }*/


   /* val schema = sql.types.StructType(List(
      sql.types.StructField("id", sql.types.DoubleType, nullable = true),
      sql.types.StructField("sim_scores", sql.types.StructType(List(
        sql.types.StructField("scores", sql.types.MapType(sql.types.StringType, sql.types.MapType(sql.types.IntegerType, sql.types.StringType)), nullable = true)
      )), nullable = true)
    ))
    val rows = Seq(
      sql.Row(1d, sql.Row(Map("topic1" -> Map(1 -> "scores1"), "topic2" -> Map(1 -> "scores2")))),
      sql.Row(2d, sql.Row(Map("topic1" -> Map(1 -> "scores1"), "topic2" -> Map(1 -> "scores2")))),
      sql.Row(3d, sql.Row(Map("topic1" -> Map(1 -> "scores1"), "topic2" -> Map(1 -> "scores2"), "topic3" -> Map(1 -> "scores3"))))
    )

    it("should flatten using dataframes and spark sql") {
      val sqlContext = new SQLContext(sc)
      val df = sqlContext.createDataFrame(sc.parallelize(rows), schema)
      df.printSchema()
      df.show()
      val numTopics = 3 // input from user
      // fancy logic to generate the select expression
      val selectColumns: Seq[String] = "id" +: 1.to(numTopics).map(i => s"sim_scores['scores']['topic${i}']")
      val df2 = df.selectExpr(selectColumns:_*)
      df2.printSchema()
      df2.show()
    }*/

    /*it("should flatten using rdd") {
      val rdd = sc.parallelize(rows)
      val out = rdd.map(row => {

      })
      df.printSchema()
      df.show()
      val df2 = df.selectExpr("id", "sim_scores['scores']['topic1']", "sim_scores['scores']['topic2']")
      df2.printSchema()
      df2.show()
    }*/

    /*it("can query a hive table in a test") {
      val session = SparkSession.builder().enableHiveSupport().getOrCreate()
      session.sql("show tables").show()
    }*/
  }
}
