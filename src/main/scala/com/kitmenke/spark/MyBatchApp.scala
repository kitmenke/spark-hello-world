package com.kitmenke.spark

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MyBatchApp {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "MyApp"

  def main(args: Array[String]): Unit = {

    try {
      val spark = SparkSession.builder()
        .appName(jobName)
        .config("spark.sql.shuffle.partitions", "3")
        .master("local[*]")
        .getOrCreate()
      import spark.implicits._

      val sentences = spark.read.csv("src/main/resources/sentences.txt").as[String]

      val counts = sentences.flatMap(splitSentenceIntoWords)
        .groupBy("value").count().sort(desc("count"))

      counts.foreach(println(_))
    } catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
  }

  def splitSentenceIntoWords(sentence: String): Array[String] = {
    sentence.split(" ").map(word => word.toLowerCase.replaceAll("[^a-z]", ""))
  }

}
