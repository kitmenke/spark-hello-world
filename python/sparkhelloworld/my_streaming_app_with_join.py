#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
An example Pyspark Structured Streaming app that reads data from Kafka
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 sparkhelloworld/my_streaming_app_with_join.py localhost:9092
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 sparkhelloworld/my_streaming_app_with_join.py $BOOTSTRAP_SERVERS
"""
import findspark
findspark.init()

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import from_json, collect_set, udf, explode, current_timestamp
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


enriched_schema = StructType() \
    .add("customer_id", StringType(), nullable=True) \
    .add("mail", StringType(), nullable=True)


def enrich_hbase(customer_ids):
    HOST='quickstart.cloudera'
    PORT=9090
    TABLE_NAME=b'kit:users'
    HBASE_CONNECTION = happybase.Connection(HOST, PORT)
    HBASE_TABLE = HBASE_CONNECTION.table(TABLE_NAME)
    print("Getting values for {0} keys".format(len(customer_ids)))
    keys = [str(cust_id).encode('utf-8') for cust_id in customer_ids]
    rows = HBASE_TABLE.rows(keys)
    result = []
    for row in rows:
        # row is a tuple that looks like
        #(b'9005729', {b'f1:mail': b'sarahschaefer@yahoo.com'})
        new_row = Row(customer_id=row[0].decode('utf-8'), mail=row[1][b'f1:mail'].decode('utf-8'))
        result.append(new_row)
    return result


if __name__ == "__main__":
    bootstrap_servers = sys.argv[1]
    print("Connecting to kafka servers %s" % (bootstrap_servers))

    spark = SparkSession \
        .builder \
        .appName("MyStreamingApp") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    # register udf
    enrich_hbase_udf = udf(lambda x: enrich_hbase(x), ArrayType(elementType=enriched_schema))
    spark.udf.register("enrich_hbase_udf", enrich_hbase_udf)

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

    df = df.select(from_json(df.value, schema).alias("raw")).selectExpr("raw.*") \
        .withColumn('current_tsp', current_timestamp()) \
        .withWatermark("current_tsp", "0 seconds")
    #df.persist()
    
    hbase_df = df.groupby("current_tsp") \
        .agg(collect_set("customer_id").alias("customer_ids")) \
        .select(enrich_hbase_udf('customer_ids').alias("enriched")) \
        .select(explode("enriched").alias("enriched_map")) \
        .select("enriched_map.customer_id", "enriched_map.mail")

    out = df.join(hbase_df, on="customer_id")

    # Start running the query that prints the running counts to the console
    query = out.writeStream \
        .outputMode('append') \
        .format('console') \
        .trigger(processingTime='10 seconds') \
        .start()

    query.awaitTermination()