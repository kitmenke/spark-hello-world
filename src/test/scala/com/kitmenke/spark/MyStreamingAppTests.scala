package com.kitmenke.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite

class MyStreamingAppTests extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._

  test("should parse json") {
    // create some sample data to run through our program
    val input = sc.parallelize(
      List[String](
    "{\"marketplace\":\"US\",\"customer_id\":1,\"review_id\":\"R26ZK6XLDT8DDS\",\"product_id\":\"B000L70MQO\",\"product_parent\":216773674,\"product_title\":\"Product 1\",\"product_category\":\"Toys\",\"star_rating\":5,\"helpful_votes\":1,\"total_votes\":4,\"vine\":\"N\",\"verified_purchase\":\"Y\",\"review_headline\":\"Five Stars\",\"review_body\":\"Cool.\",\"review_date\":\"2015-01-12T00:00:00.000-06:00\"}\n",
        "{\"marketplace\":\"US\",\"customer_id\":2,\"review_id\":\"R3N4AL9Y48M9HB\",\"product_id\":\"B002SW4856\",\"product_parent\":914652864,\"product_title\":\"Product 2\",\"product_category\":\"Toys\",\"star_rating\":1,\"helpful_votes\":2,\"total_votes\":5,\"vine\":\"N\",\"verified_purchase\":\"Y\",\"review_headline\":\"1 Star\",\"review_body\":\"Bad.\",\"review_date\":\"2015-01-12T00:00:00.000-06:00\"}\n",
        "{\"marketplace\":\"US\",\"customer_id\":3,\"review_id\":\"R1UZ1DLRGUY2BT\",\"product_id\":\"B00IR7NKWS\",\"product_parent\":301509474,\"product_title\":\"Product 2\",\"product_category\":\"Toys\",\"star_rating\":5,\"helpful_votes\":3,\"total_votes\":6,\"vine\":\"N\",\"verified_purchase\":\"Y\",\"review_headline\":\"Five Stars\",\"review_body\":\"Great!\",\"review_date\":\"2015-01-12T00:00:00.000-06:00\"}\n"
    )).toDF("value")

    val expected = sc.parallelize(List(Some(3.67))).toDF("avg_star_rating")
    val actual = MyStreamingApp.compute(input)
    actual.printSchema()
    actual.show()
    assertDataFrameEquals(actual, expected) // equal
  }
}
