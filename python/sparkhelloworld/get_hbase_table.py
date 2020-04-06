#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Get an hbase table in a dataframe

Run this job using:
spark-submit --files sparkhelloworld/hbase-site.xml --packages org.apache.hbase.connectors.spark:hbase-spark:1.0.0,org.apache.hbase:hbase-common:2.2.3,org.apache.hbase:hbase-client:2.2.3,org.apache.hbase:hbase-shaded-mapreduce:2.2.3 sparkhelloworld/get_hbase_table.py

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

    #spark.sparkContext.setLogLevel('WARN')
    # hbase.spark.use.hbasecontext = False to create a new configuration otherwise it tries to read from cache which is
    #    null and results in a NPE
    df = spark.read.format('org.apache.hadoop.hbase.spark') \
    .option('hbase.spark.use.hbasecontext', True) \
    .option('hbase.spark.config.location', 'file:///home/kit/Code/spark-hello-world/python/sparkhelloworld/hbase-site.xml') \
    .option('hbase.config.resources', 'file:///home/kit/Code/spark-hello-world/python/sparkhelloworld/hbase-site.xml') \
    .option('hbase.table','kit:users') \
    .option('hbase.columns.mapping', 'id STRING :key, mail STRING f1:mail') \
    .load()

    df.filter("id = '51830937'").show()
