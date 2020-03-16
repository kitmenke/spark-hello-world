#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
An example Pyspark Structured Streaming app that reads data from Kafka

Run using:
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 spark-hello-world/my_streaming_app.py
"""
import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import *



def compute(df):
    """
    Parse json and sum all the random-ints
    :type df: DataFrame
    """
    schema = StructType([StructField("random-string", StringType(), nullable=True),
                         StructField("random-int", IntegerType(), nullable=True)])
    return df.select(from_json(df.value, schema).alias("js")) \
        .agg(spark_sum("js.random-int").alias("s"))


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("MyStreamingApp") \
        .getOrCreate()

    # Create DataFrame with (key, value)
    df = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'localhost:9092') \
        .option('subscribe', 'test') \
        .option('startingOffsets', 'earliest') \
        .load() \
        .selectExpr('CAST(key AS STRING)', 'CAST(value AS STRING)')
    df.printSchema()

    out = compute(df)

    # Start running the query that prints the running counts to the console
    query = out.writeStream \
        .outputMode('complete') \
        .format('console') \
        .start()

    query.awaitTermination()