import pytest
from pyspark.sql import Row
import sparkhelloworld

pytestmark = pytest.mark.usefixtures("spark_context")


def test_parse_json_and_sum_random_ints(spark_context):
    """ test json parsing
    Args:
       spark_context: test fixture SparkContext
    """
    data = [Row(key=1, value="{ \"random-name\": \"Bob\", \"random-int\": 42 }"),
        Row(key=1, value="{ \"random-name\": \"Mary\", \"random-int\": 100 }")]
    df = spark_context.sparkContext.parallelize(data).toDF()
    actual = sparkhelloworld.my_streaming_app.compute(df)
    assert actual.collect()[0].s == 142