package com.kitmenke.spark

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

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
        .load()

      val query = df.writeStream
        .outputMode(OutputMode.Append())
        .format("console")
        .start()

      query.awaitTermination()
    } catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
  }
}
