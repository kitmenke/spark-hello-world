package com.kitmenke.spark.streaming

import java.util.{Date, UUID}

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.log4j.Logger
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.KinesisInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

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
    kinesisStream.foreachRDD(rdd => writeRddToSolr(sparkSession, rdd))
    ssc.start()
    ssc.awaitTermination()
  }

  def writeRddToSolr(spark: SparkSession, rdd: RDD[Array[Byte]]): Unit = {
    val rows: RDD[String] = rdd.map(bytes => new String(bytes))
    val df = jsonToDataframe(spark, rows)
    df.foreachPartition(iter => {
      val connection = new ConcurrentUpdateSolrClient.Builder("http://localhost:8983/solr/gettingstarted").build()
      iter.foreach(row => {
        connection.add(rowToSolrDocument(row))
      })
      connection.commit()
      connection.close()
    })
  }

  val twilioJsonSchema: StructType = StructType(List(
    StructField("FromZip", StringType, nullable = true),
    StructField("FromCity", StringType, nullable = true),
    StructField("FromState", StringType, nullable = true),
    StructField("Body", StringType, nullable = true)
  ))

  def jsonToDataframe(sparkSession: SparkSession, rdd: RDD[String]): DataFrame = {
    val rows = rdd.map(s => sql.Row(s))
    val schema = StructType(Seq(StructField("twilio_json", StringType, nullable = true)))
    val df = sparkSession.createDataFrame(rows, schema)
    val df2 = df.select(sql.functions.from_json(df("twilio_json"), twilioJsonSchema).as("parsed_json"))
    df2.select("parsed_json.*")
  }

  def rowToSolrDocument(row: Row): SolrInputDocument = {
    val doc = new SolrInputDocument()
    doc.setField("id", UUID.randomUUID().toString)
    doc.setField("Body", row.getAs[String]("Body"))
    doc.setField("FromZip", row.getAs[String]("FromZip"))
    doc.setField("FromCity", row.getAs[String]("FromCity"))
    doc.setField("FromState", row.getAs[String]("FromState"))
    doc.setField("sentiment", "Neutral")
    doc.setField("created", new Date())
    doc
  }
}
