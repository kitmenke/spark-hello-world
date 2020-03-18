# PySpark hello world app

An example python pyspark Structured Streaming app with unit tests.

- Must have Java 8 installed. *Java 11 will not work*.
- Must have spark 2.4.5 installed.

Set SPARK_HOME environment variable.

```
# For Mac
export SPARK_HOME=/usr/local/Cellar/apache-spark/2.4.5/libexec/
# For Linux
export SPARK_HOME="/home/username/spark-2.4.5-bin-hadoop2.7"
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