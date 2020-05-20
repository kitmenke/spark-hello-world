# PySpark hello world app

An example python pyspark Structured Streaming app with unit tests.

- Must have Java 8 installed. *Java 11 will not work*.
- Must have spark 2.4.5 installed ( https://spark.apache.org/downloads.html )

Set SPARK_HOME environment variable to where Spark is installed on your machine

```
# Example
export SPARK_HOME="/home/username/spark-2.4.5-bin-hadoop2.7"

# Example if used homebrew for installation
export SPARK_HOME=/usr/local/Cellar/apache-spark/2.4.5/libexec/
```

To run the app, you must have installed Apache Spark 2.4.5 somewhere on your system.
```
# install the dependencies
make init
# set env var to your kafka bootstrap servers
export BOOTSTRAP_SERVERS="localhost:9092"
# run the app 
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 sparkhelloworld/my_streaming_app.py $BOOTSTRAP_SERVERS
```
# Hive

```
CREATE EXTERNAL TABLE reviews (
marketplace STRING,
customer_id BIGINT,
review_id STRING,
product_id STRING,
product_parent INT,
product_title STRING,
product_category STRING,
star_rating INT,
helpful_votes INT,
total_votes INT,
vine STRING,
verified_purchase STRING,
review_headline STRING,
review_body STRING,
review_date TIMESTAMP,
current_tsp TIMESTAMP
)
STORED AS PARQUET
LOCATION '/user/kit/reviews';
```

To check that it's processing data, add the startingOffsets option to the `my_streaming_app.py` file and rerun your spark-submit job.
```
# Create DataFrame with (key, value)
    df = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', bootstrap_servers) \
        .option('subscribe', 'reviews') \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr('CAST(value AS STRING)')
```

# Tests

The tests use pytest.

```
# run the tests once
pytest
```

To run the tests in continuous mode:
```
make test
```

Testing based largely on this blog:
https://engblog.nextdoor.com/unit-testing-apache-spark-with-py-test-3b8970dc013b

# Troubleshooting

Can findspark find your spark installation? If not, do you have the `SPARK_HOME` environment variable set?

```
(env1) kit@spectre:~/Code/spark-hello-world/python$ python
Python 3.7.5 (default, Nov 20 2019, 09:21:52) 
[GCC 9.2.1 20191008] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> import findspark
>>> findspark.init()
>>> findspark.find()
'/home/kit/spark-2.4.5-bin-hadoop2.7'
```