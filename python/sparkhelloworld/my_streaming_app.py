#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
An example Pyspark Structured Streaming app that reads data from Kafka
"""
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys


schema = StructType() \
    .add("marketplace", StringType(), nullable=True) \
    .add("customer_id", IntegerType(), nullable=True) \
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

def compute(df):
    """
    Parse json and get average star rating
    :type df: DataFrame
    """

    return df.select(from_json(df.value, schema).alias("js")) \
        .agg(round(avg("js.star_rating"), 2).alias("avg_star_rating"))


if __name__ == "__main__":
    bootstrap_servers = sys.argv[1]
    print("Connecting to kafka servers %s" % (bootstrap_servers))

    spark = SparkSession \
        .builder \
        .appName("MyStreamingApp") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    # Create DataFrame with (key, value)
    df = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', bootstrap_servers) \
        .option('subscribe', 'reviews') \
        .load() \
        .selectExpr('CAST(value AS STRING)')

    df.printSchema()

    out = compute(df)

    # Start running the query that prints the running counts to the console
    query = out.writeStream \
        .outputMode('complete') \
        .format('console') \
        .trigger(processingTime='5 seconds') \
        .start()

    query.awaitTermination()