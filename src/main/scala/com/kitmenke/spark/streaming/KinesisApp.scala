package com.kitmenke.spark.streaming

import java.util.{Date, UUID}

import com.amazonaws.services.comprehend.AmazonComprehendClientBuilder
import com.amazonaws.services.comprehend.model.BatchDetectSentimentRequest
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.log4j.Logger
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.KinesisInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

case class TwilioMessage(body: String, zip: String, city: String, state: String)

object KinesisApp {
  lazy val LOG = Logger.getLogger(this.getClass)
  val jobName = "KinesisApp"

  def main(args: Array[String]): Unit = {
    // Create a local spark session and streaming context
    val master = "local[*]"
    val interval = Seconds(5)
    val sparkSession = SparkSession.builder().appName(jobName).master(master).getOrCreate()
    val ssc = new StreamingContext(sparkSession.sparkContext, interval)

    // Note: You must setup your AWS Credentials before this will work
    // See http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html
    val kinesisStream = KinesisInputDStream.builder
      .streamingContext(ssc)
      .endpointUrl("https://kinesis.us-east-1.amazonaws.com")
      .regionName("us-east-1")
      .streamName("DE_April_Meetup_KS0")
      .checkpointAppName(jobName)
      .checkpointInterval(interval)
      .storageLevel(StorageLevel.MEMORY_AND_DISK_2)
      .build()
    kinesisStream.foreachRDD(rdd => process(rdd))
    ssc.start()
    ssc.awaitTermination()
  }

  def process(rdd: RDD[Array[Byte]]): Unit = {
    val messages = parseJson(rdd)
    // lookup sentiment for each message
    val sentiment = addSentiment(messages)
    // write the result to solr
    writeToSolr(sentiment)
  }

  def parseJson(rdd: RDD[Array[Byte]]): RDD[TwilioMessage] = {
    // parse the json string and extract just the columns we're interested in
    rdd.mapPartitions(iter => {
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      iter.map(bytes => {
        val map = mapper.readValue(bytes, classOf[Map[String,String]])
        TwilioMessage(map("Body"), map("FromZip"), map("FromCity"), map("FromState"))
      })
    })
  }

  def addSentiment(rdd: RDD[TwilioMessage]): RDD[(TwilioMessage, String)] = {
    rdd.mapPartitions(iter => {
      val client = AmazonComprehendClientBuilder.defaultClient()
      try {
        iter
          .grouped(25)
          .flatMap(messages => {
            val request = new BatchDetectSentimentRequest()
            import scala.collection.JavaConversions._
            request.withTextList(messages.map(m => m.body))
            val result = client.batchDetectSentiment(request)
            messages.zip(result.getResultList.map(res => res.getSentiment))
          })
      } finally {
        client.shutdown()
      }
    })
  }

  def writeToSolr(rdd: RDD[(TwilioMessage, String)]): Unit = {
    rdd.foreachPartition(iter => {
      val connection = new ConcurrentUpdateSolrClient.Builder("http://localhost:8983/solr/gettingstarted").build()
      iter.foreach { case (msg, sentiment) =>
        connection.add(rowToSolrDocument(msg.body, msg.zip, msg.city, msg.state, sentiment))
      }
      connection.commit()
      connection.close()
    })
  }

  def rowToSolrDocument(body: String, zip: String, city: String, state: String, sentiment: String): SolrInputDocument = {
    val doc = new SolrInputDocument()
    doc.setField("id", UUID.randomUUID().toString)
    doc.setField("Body", body)
    doc.setField("FromZip", zip)
    doc.setField("FromCity", city)
    doc.setField("FromState", state)
    doc.setField("sentiment", sentiment)
    doc.setField("created", new Date())
    doc
  }


}
