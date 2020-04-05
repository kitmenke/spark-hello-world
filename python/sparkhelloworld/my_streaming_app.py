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
import happybase


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


def enrich_hbase(customer_id):
    HOST='quickstart.cloudera'
    PORT=9090
    TABLE_NAME=b'kit:users'
    HBASE_CONNECTION = happybase.Connection(HOST, PORT)
    HBASE_TABLE = HBASE_CONNECTION.table(TABLE_NAME)
    print("Getting values for row key '{0}'".format(customer_id))
    row = HBASE_TABLE.row(str(customer_id).encode('utf-8'))
    print(row)
    # copy stuff over
    if b'f1:mail' in row:
        return row[b'f1:mail'].decode('utf-8')
    else:
        return None


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

    step1 = df.select(from_json(df.value, schema).alias("js")).select("js.*")
    step1.printSchema()
    
    enrich_hbase_udf = udf(lambda id: enrich_hbase(id), StringType())
    spark.udf.register("enrich_hbase_udf", enrich_hbase_udf)

    out = step1.withColumn('mail', enrich_hbase_udf('customer_id'))
    out.printSchema

    # Start running the query that prints the running counts to the console
    query = out.writeStream \
        .outputMode('append') \
        .format('console') \
        .trigger(processingTime='5 seconds') \
        .start()

    query.awaitTermination()