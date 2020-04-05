#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Get an hbase table in a dataframe
org.apache.hbase.connectors.spark
hbase-spark
1.0.0
spark-submit --packages org.apache.hbase.connectors.spark:hbase-spark:1.0.0,org.apache.hbase:hbase-common:2.2.3,org.apache.hbase:hbase-client:2.2.3,org.apache.hbase:hbase-shaded-mapreduce:2.2.3 sparkhelloworld/get_hbase_table.py
"""
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import happybase


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("KitGetHbaseTableAsDataframe") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    df = spark.read.format('org.apache.hadoop.hbase.spark') \
    .option("hbase.spark.use.hbasecontext", False) \
    .option("hbase.zookeeper.quorum", "35.184.255.239:2181") \
    .option('hbase.table','kit:users') \
    .option('hbase.columns.mapping', 'id STRING :key, mail STRING f1:mail') \
    .load()

    df.filter("id = '51830937'").show()
