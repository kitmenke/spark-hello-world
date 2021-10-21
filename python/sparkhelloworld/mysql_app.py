#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
An example Pyspark app that reads from mysql

MySQL running in docker
https://hub.docker.com/r/genschsa/mysql-employees

"""
import findspark
findspark.init()

from pyspark.sql import SparkSession
import sys

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("MySQLApp") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    # needed to include autoReconnect=true&useSSL=false to disable SSL
    # because I'm running mysql out of a local docker container
    # and I get errors otherwise
    employees_df = spark.read.format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/employees?autoReconnect=true&useSSL=false") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "employees") \
        .option("user", "root").option("password", "college").load()

    employees_df.printSchema()
    employees_df.show()