package com.kitmenke.spark

import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

/**
 * A batch application that takes a hard-coded list of strings and counts the words.
 */
object MyBatchApp {
  lazy val logger: Logger = LogManager.getLogger(this.getClass)
  val jobName = "MyBatchApp"

  def main(args: Array[String]): Unit = {
    logger.info(s"$jobName starting")
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
      // create our spark session which will run locally
      val spark = SparkSession.builder().appName(jobName).master("local[*]").getOrCreate()
      // create an RDD using our test data
      val rdd = spark.sparkContext.parallelize(sentences)
      // main app logic:
      // 1. take all of the sentences and split them into words
      // 2. smash all the words into one long list, where each element is one word
      // 3. transform each word into a tuple containing that word and the number 1 (word, 1)
      // 4. for every unique word, sum up all the 1s to get the number of times that word appears
      // 5. sort by word count, descending
      val counts = rdd.flatMap(splitSentenceIntoWords)
        .map(word => (word, 1))
        .reduceByKey(_ + _)
        .sortBy(t => t._2, ascending = false)
      val output = counts.collect().mkString("\n")
      logger.info("Counts are: " + output)
    } catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
    logger.info(s"$jobName finished")
  }

  def splitSentenceIntoWords(sentence: String): Array[String] = {
    sentence.split(" ").map(word => word.toLowerCase.replaceAll("[^a-z]", ""))
  }

}
