package com.kitmenke.spark.streaming

import java.util.{Date, UUID}

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.log4j.Logger
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.KinesisInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KinesisApp {
  lazy val LOG = Logger.getLogger(this.getClass)
  val jobName = "KinesisApp"

  def main(args: Array[String]): Unit = {
    val master = "local[*]"
    val interval = Seconds(5)
    val conf = new SparkConf().setAppName(jobName).setMaster(master)
    val ssc = new StreamingContext(conf, interval)

    // Note: You must setup your AWS Credentials before this will work
    // See http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html
    val kinesisStream = KinesisInputDStream.builder
      .streamingContext(ssc)
      .endpointUrl("https://kinesis.us-east-1.amazonaws.com")
      .regionName("us-east-1")
      .streamName("DE_April_Meetup_KS0")
      .initialPositionInStream(InitialPositionInStream.LATEST)
      .checkpointAppName(jobName)
      .checkpointInterval(interval)
      .storageLevel(StorageLevel.MEMORY_AND_DISK_2)
      .build()
    kinesisStream.foreachRDD(rdd => writeRddToSolr(rdd))
    ssc.start()
    ssc.awaitTermination()
  }

  def writeRddToSolr(rdd: RDD[Array[Byte]]): Unit = {

    rdd.foreachPartition(iter => {
      val connection = new ConcurrentUpdateSolrClient.Builder("http://localhost:8983/solr/gettingstarted").build()
      iter.foreach(bytes => {
        val value = new String(bytes)
        val doc = new SolrInputDocument()
        doc.setField("id", UUID.randomUUID().toString)
        doc.setField("message", value)
        doc.setField("created", new Date())
        connection.add(doc)
      })
      connection.commit()
      connection.close()
    })
  }
}
