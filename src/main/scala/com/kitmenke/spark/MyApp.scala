package com.kitmenke.spark

import java.util.{Date, UUID}

import org.apache.log4j.Logger
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.sql.SparkSession

//https://github.com/lucidworks/banana/archive/v1.6.20.tar.gz
//https://hub.docker.com/_/solr/
//docker run --name my_solr -d -p 8983:8983 -t solr
object MyApp {
  lazy val LOG = Logger.getLogger(this.getClass)
  val jobName = "MyApp"

  def main(args: Array[String]): Unit = {
    try {
      val spark = SparkSession.builder().appName(jobName).master("local[1]").getOrCreate()
      val rdd = spark.sparkContext.textFile("/home/kit/Downloads/pg50133.txt")
      val counts = rdd.flatMap(splitSentenceIntoWords)
        .map(word => (word, 1))
        .reduceByKey((a,b) => a + b)
        .sortBy(t => t._2, ascending = false)
      counts.foreachPartition((iter: Iterator[(String, Int)]) => {
        val connection = new ConcurrentUpdateSolrClient.Builder("http://localhost:8983/solr/gettingstarted").build()
        iter.foreach { case (word, count) => {
          val doc = new SolrInputDocument
          doc.addField("id", word)
          doc.addField("count_l", count)
          doc.addField("last_updated_dt", new Date())
          connection.add(doc)
        }}
        connection.commit()
        connection.close()
      })
    } catch {
      case e: Exception => LOG.error(s"$jobName error in main", e)
    }
  }

  def splitSentenceIntoWords(sentence: String): Array[String] = {
    sentence.split(" ").map(word => word.trim().toLowerCase.replaceAll("[^a-z]", "")).filter(p => p.length > 0)
  }

}
