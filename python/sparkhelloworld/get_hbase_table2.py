#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Get an hbase table in a dataframe
org.apache.hbase.connectors.spark
hbase-spark
1.0.0
spark-submit sparkhelloworld/get_hbase_table2.py
"""
import findspark
findspark.init()

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import from_json, collect_set, udf, explode
from pyspark.sql.types import StructType, ArrayType, MapType, StringType
import sys
import happybase


schema = StructType() \
    .add("customer_id", StringType(), nullable=True) \
    .add("mail", StringType(), nullable=True)


def enrich_hbase(customer_ids):
    HOST='quickstart.cloudera'
    PORT=9090
    TABLE_NAME=b'kit:users'
    HBASE_CONNECTION = happybase.Connection(HOST, PORT)
    HBASE_TABLE = HBASE_CONNECTION.table(TABLE_NAME)
    print("Getting values for {0} keys".format(len(customer_ids)))
    keys = [cust_id.encode('utf-8') for cust_id in customer_ids]
    rows = HBASE_TABLE.rows(keys)
    result = []
    for row in rows:
        # row is a tuple that looks like
        #(b'9005729', {b'f1:mail': b'sarahschaefer@yahoo.com'})
        new_row = Row(customer_id=row[0].decode('utf-8'), mail=row[1][b'f1:mail'].decode('utf-8'))
        result.append(new_row)
    return result


""" 
Take the list of customer IDs
+-----------+
|customer_id|
+-----------+
|    9005729|
|     500600|
|   30059640|
|    6005263|
|     800182|
+-----------+

Smash them down into one row
+--------------------+
|        customer_ids|
+--------------------+
|[500600, 6005263,...|
+--------------------+

Enrich them with the data from hbase. The type is a bit complicated here:
Array(Row(customer_id, mail))
+--------------------+
|            enriched|
+--------------------+
|[[500600, cholmes...|
+--------------------+

Explode the array (each element becomes its own row)
+--------------------+
|        enriched_map|
+--------------------+
|[500600, cholmes@...|
|[6005263, jasmine...|
|[9005729, sarahsc...|
|[30059640, xmitch...|
|[800182, shawnlew...|
+--------------------+ 

Pull the elements from the map back out into separate columns
+-----------+--------------------+
|customer_id|                mail|
+-----------+--------------------+
|     500600|   cholmes@yahoo.com|
|    6005263|jasmine51@hotmail...|
|    9005729|sarahschaefer@yah...|
|   30059640|xmitchell@hotmail...|
|     800182|shawnlewis@hotmai...|
+-----------+--------------------+
"""

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("KitGetHbaseTableAsDataframe") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')
    #val list = List(new Get("9005729"), new Get("500600"), new Get("30059640"), new Get("6005263"), new Get("800182")).asJava
    data = [Row(customer_id="9005729"),
            Row(customer_id="500600"),
            Row(customer_id="30059640"),
            Row(customer_id="6005263"),
            Row(customer_id="800182")
            ]
    enrich_hbase_udf = udf(lambda x: enrich_hbase(x), ArrayType(elementType=schema))
    spark.udf.register("enrich_hbase_udf", enrich_hbase_udf)

    df = spark.sparkContext.parallelize(data).toDF()
    df.agg(collect_set("customer_id").alias("customer_ids")) \
        .select(enrich_hbase_udf('customer_ids').alias("enriched")) \
        .select(explode("enriched").alias("enriched_map")) \
        .select("enriched_map.customer_id", "enriched_map.mail") \
        .show()