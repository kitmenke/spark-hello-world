# PySpark hello world app

An example python pyspark Structured Streaming app with unit tests.

To run the app, you must have installed Apache Spark 2.4.5 somewhere on your system.
```
# install the dependencies
make init
# 
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 sparkhelloworld/my_streaming_app.py
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