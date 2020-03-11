package com.kitmenke.spark

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object MyApp {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "MyApp"

  def main(args: Array[String]): Unit = {
    val sentences = Seq(
      "Space.",
      "The final frontier.",
      "These are the voyages of the starship Enterprise.",
      "Its continuing mission:",
      "to explore strange new worlds,",
      "to seek out new life and new civilizations,",
      "to boldly go where no one has gone before!"
    )

    try {
      val spark = SparkSession.builder().appName(jobName).master("local[1]").getOrCreate()
      val rdd = spark.sparkContext.parallelize(sentences)
      val counts = rdd.flatMap(splitSentenceIntoWords)
        .map(word => (word, 1))
        .reduceByKey(_ + _)
        .sortBy(t => t._2, ascending = false)
      counts.foreach(println)
    } catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
  }

  def splitSentenceIntoWords(sentence: String): Array[String] = {
    sentence.split(" ").map(word => word.toLowerCase.replaceAll("[^a-z]", ""))
  }

}
