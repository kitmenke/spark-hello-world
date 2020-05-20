#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
An example Pyspark Structured Streaming app that reads data from Kafka
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 sparkhelloworld/my_streaming_app.py localhost:9092
spark-submit --conf spark.hadoop.dfs.client.use.datanode.hostname=true --conf spark.hadoop.fs.defaultFS=hdfs://quickstart.cloudera:8020 --conf spark.hadoop.dfs.namenode.rpc-address=quickstart.cloudera:8020 --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 sparkhelloworld/my_streaming_app.py $BOOTSTRAP_SERVERS
--conf spark.hadoop.dfs.replication=0 

"""
import findspark
findspark.init()

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import from_json, collect_set, udf, explode, current_timestamp
from pyspark.sql.types import *
import sys


schema = StructType() \
    .add("marketplace", StringType(), nullable=True) \
    .add("customer_id", LongType(), nullable=True) \
    .add("review_id", StringType(), nullable=True) \
    .add("product_id", StringType(), nullable=True) \
    .add("product_parent", IntegerType(), nullable=True) \
    .add("product_title", StringType(), nullable=True) \
    .add("product_category", StringType(), nullable=True) \
    .add("star_rating", IntegerType(), nullable=True) \
    .add("helpful_votes", IntegerType(), nullable=True) \
    .add("total_votes", IntegerType(), nullable=True) \
    .add("vine", StringType(), nullable=True) \
    .add("verified_purchase", StringType(), nullable=True) \
    .add("review_headline", StringType(), nullable=True) \
    .add("review_body", StringType(), nullable=True) \
    .add("review_date", TimestampType(), nullable=True)


if __name__ == "__main__":
    bootstrap_servers = sys.argv[1]
    print("Connecting to kafka servers %s" % (bootstrap_servers))

    spark = SparkSession \
        .builder \
        .appName("KitsStreamingApp") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    # Create DataFrame with (key, value)
    df = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', bootstrap_servers) \
        .option('subscribe', 'reviews') \
        .option('maxOffsetsPerTrigger', 10) \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr('CAST(value AS STRING)')

    out = df.select(from_json(df.value, schema).alias("raw")).selectExpr("raw.*") \
        .withColumn('current_tsp', current_timestamp()) 

    # Start running the query that prints the running counts to the console
    query = out.writeStream \
        .format("parquet") \
        .option("path", "/user/kit/reviews") \
        .option("checkpointLocation", "/user/kit/reviews_checkpoint") \
        .trigger(processingTime='10 seconds') \
        .outputMode("append") \
        .start()

    query.awaitTermination()