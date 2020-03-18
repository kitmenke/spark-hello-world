import pytest
from pyspark.sql import Row
import sparkhelloworld

pytestmark = pytest.mark.usefixtures("spark_context")


def test_parse_json_and_sum_random_ints(spark_context):
    """ test json parsing
    Args:
       spark_context: test fixture SparkContext
    """
    data = [Row(value="{\"marketplace\":\"US\",\"customer_id\":1,\"review_id\":\"R26ZK6XLDT8DDS\",\"product_id\":\"B000L70MQO\",\"product_parent\":216773674,\"product_title\":\"Product 1\",\"product_category\":\"Toys\",\"star_rating\":5,\"helpful_votes\":1,\"total_votes\":4,\"vine\":\"N\",\"verified_purchase\":\"Y\",\"review_headline\":\"Five Stars\",\"review_body\":\"Cool.\",\"review_date\":\"2015-01-12T00:00:00.000-06:00\"}\n"),
            Row(value="{\"marketplace\":\"US\",\"customer_id\":2,\"review_id\":\"R3N4AL9Y48M9HB\",\"product_id\":\"B002SW4856\",\"product_parent\":914652864,\"product_title\":\"Product 2\",\"product_category\":\"Toys\",\"star_rating\":1,\"helpful_votes\":2,\"total_votes\":5,\"vine\":\"N\",\"verified_purchase\":\"Y\",\"review_headline\":\"1 Star\",\"review_body\":\"Bad.\",\"review_date\":\"2015-01-12T00:00:00.000-06:00\"}\n"),
            Row(value="{\"marketplace\":\"US\",\"customer_id\":3,\"review_id\":\"R1UZ1DLRGUY2BT\",\"product_id\":\"B00IR7NKWS\",\"product_parent\":301509474,\"product_title\":\"Product 2\",\"product_category\":\"Toys\",\"star_rating\":5,\"helpful_votes\":3,\"total_votes\":6,\"vine\":\"N\",\"verified_purchase\":\"Y\",\"review_headline\":\"Five Stars\",\"review_body\":\"Great!\",\"review_date\":\"2015-01-12T00:00:00.000-06:00\"}\n"),
            ]
    df = spark_context.sparkContext.parallelize(data).toDF()
    actual = sparkhelloworld.my_streaming_app.compute(df)
    assert actual.collect()[0].avg_star_rating == 3.67