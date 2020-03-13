package com.kitmenke.spark

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{from_json, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object MyStreamingApp {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "MyStreamingApp"

  def main(args: Array[String]): Unit = {
    try {
      val spark = SparkSession.builder().appName(jobName).master("local[*]").getOrCreate()

      val df = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "test")
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      df.printSchema()

      val out = compute(df)

      val query = out.writeStream
        .outputMode(OutputMode.Complete())
        .format("console")
        .start()

      query.awaitTermination()
    } catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
  }

  def compute(df: DataFrame): DataFrame = {
    val schema = StructType(List(
      StructField("random-name", StringType, nullable = true),
      StructField("random-int", IntegerType, nullable = true)
    ))
    df.select(from_json(df("value"), schema) as "js")
      .agg(sum("js.random-int") as "s")
  }
}
