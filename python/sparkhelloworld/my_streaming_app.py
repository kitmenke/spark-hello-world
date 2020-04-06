#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
An example Pyspark Structured Streaming app that reads data from Kafka
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.5 sparkhelloworld/my_streaming_app.py localhost:2181
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:2.4.5 sparkhelloworld/my_streaming_app.py localhost:2181
"""
import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import sys
import happybase

""" 
def enrich_hbase(rdd):
    HOST='quickstart.cloudera'
    PORT=9090
    TABLE_NAME=b'kit:users'
    HBASE_CONNECTION = happybase.Connection(HOST, PORT)
    HBASE_TABLE = HBASE_CONNECTION.table(TABLE_NAME)
    print("Getting values for {0} keys".format(len(rdd)))
    for record in rdd:
    keys = [str(cust_id).encode('utf-8') for cust_id in customer_ids]
    rows = HBASE_TABLE.rows(keys)
    result = []
    for row in rows:
        # row is a tuple that looks like
        #(b'9005729', {b'f1:mail': b'sarahschaefer@yahoo.com'})
        new_row = Row(customer_id=row[0].decode('utf-8'), mail=row[1][b'f1:mail'].decode('utf-8'))
        result.append(new_row)
    return result """



def print_rdd(rdd):
    print("RDD:")
    print("------------------------------------")
    rdd.foreach(lambda x: print(x))
    print("------------------------------------")


if __name__ == "__main__":
    zk_quorum = sys.argv[1]
    print("Connecting to zk quorum %s" % (zk_quorum))

    # Create a local StreamingContext with two working thread and batch interval of 1 second
    sc = SparkContext("local[*]", "KitDstreamApp")
    sc.setLogLevel('WARN')
    ssc = StreamingContext(sc, 10)
    kafkaStream = KafkaUtils.createStream(ssc, zk_quorum, 'KitDstreamApp', {'reviews': 1}, kafkaParams={"startingOffsets": "earliest"})
    kafkaStream.foreachRDD(print_rdd)
    #rdd = kafkaStream.mapPartitions(enrich_hbase)

    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate