"""
 From: https://github.com/kawadia/pyspark.test/blob/master/examples/conftest.py
 pytest fixtures that can be resued across tests. the filename needs to be conftest.py
"""

# make sure env variables are set correctly
import findspark  # this needs to be the first import
findspark.init()

import logging
import pytest

from pyspark import HiveContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext


def quiet_py4j():
    """ turn down spark logging for the test context """
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_context(request):
    """ fixture for creating a spark context
    Args:
        request: pytest.FixtureRequest object

    """
    spark = SparkSession \
        .builder \
        .appName("pytest-pyspark-local-testing") \
        .master("local[2]") \
        .getOrCreate()
    request.addfinalizer(lambda: spark.stop())

    quiet_py4j()
    return spark


@pytest.fixture(scope="session")
def hive_context(spark_context):
    """  fixture for creating a Hive Context. Creating a fixture enables it to be reused across all
        tests in a session
    Args:
        spark_context: spark_context fixture

    Returns:
        HiveContext for tests

    """
    return HiveContext(spark_context)


@pytest.fixture(scope="session")
def streaming_context(spark_context):
    return StreamingContext(spark_context, 1)