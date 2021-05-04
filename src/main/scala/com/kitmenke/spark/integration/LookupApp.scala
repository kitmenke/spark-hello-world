package com.kitmenke.spark.integration

import com.codahale.metrics.jmx.JmxReporter
import com.datastax.driver.core.{Cluster, Session}
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}



/**
 * A batch application that reads data from a file and joins it with data in a database.
 */
object LookupApp {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "LookupApp"
  val userSchema: StructType = new StructType()
    .add("id", StringType, nullable = false)
    .add("interactions_with_q", IntegerType, nullable = false)

  def main(args: Array[String]): Unit = {
    try {
      // create our spark session which will run locally
      val spark = SparkSession.builder().appName(jobName).master("local[*]").getOrCreate()
      val sc = spark.sparkContext

      val df = spark.read.schema(userSchema).json("src/main/resources/data.json")
      val ds = enrichFromDatabase(spark, df, "localhost")
      ds.printSchema()
      ds.show()
    } catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
  }

  def enrichFromDatabase(sparkSession: SparkSession, df: DataFrame, cassandraHost: String): Dataset[EnrichedUser] = {
    import sparkSession.implicits._
    df.mapPartitions(partition => {
      val cluster = Cluster.builder().addContactPoint(cassandraHost).build()
      var session: Session = null
      try {
        session = cluster.connect()
        partition.map(row => {
          val userId = row.getString(0)
          val resultSet = session.execute(s"select * from test.user where id = '$userId';")
          val result = resultSet.one()

          EnrichedUser(userId, result.getString("name"), result.getString("email"))
        })
      } finally {
        if (session != null) session.close()
      }
    })
  }

}
