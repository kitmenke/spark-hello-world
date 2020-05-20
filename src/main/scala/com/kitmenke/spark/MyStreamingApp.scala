package com.kitmenke.spark

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Spark Structured Streaming app
 *
 * Takes one argument, for Kafka bootstrap servers (ex: localhost:9092)
 */
object MyStreamingApp {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "MyStreamingApp"
  // TODO: define the schema for parsing data from Kafka
  val schema: StructType = ???

  def main(args: Array[String]): Unit = {
    try {
      val spark = SparkSession.builder().appName(jobName).master("local[*]").getOrCreate()
      val bootstrapServers = args(0)
      val df = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("subscribe", "reviews")
        .load()
        .selectExpr("CAST(value AS STRING)")

      df.printSchema()

      val out = compute(df)

      val query = out.writeStream
        .outputMode(OutputMode.Complete())
        .format("console")
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .start()

      query.awaitTermination()
    } catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
  }

  def compute(df: DataFrame): DataFrame = {
    // TODO: implement your logic here
    df
  }
}
