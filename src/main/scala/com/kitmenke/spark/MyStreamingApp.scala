package com.kitmenke.spark

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.SparkSession

/**
 * Spark Structured Streaming app
 *
 * Takes one argument, for Kafka bootstrap servers (ex: localhost:9092)
 */
object MyStreamingApp {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "MyStreamingApp"
  // TODO: define the schema for parsing data from Kafka

  def main(args: Array[String]): Unit = {
    try {

      val spark = SparkSession.builder()
        .appName(jobName)
        .config("spark.sql.shuffle.partitions", "3")
        .master("local[*]")
        .getOrCreate()
      import spark.implicits._

      val bootstrapServers = args(0)

      val sentences = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("subscribe", "word-count")
        .load()
        .selectExpr("CAST(value AS STRING)").as[String]

      /*val counts = ???

      val query = counts.writeStream
        .outputMode(OutputMode.Complete())
        .format("console")
        .trigger(Trigger.ProcessingTime("30 seconds"))
        .start()

      query.awaitTermination()*/
    } catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
  }

}
