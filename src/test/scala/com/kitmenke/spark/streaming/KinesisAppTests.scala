package com.kitmenke.spark.streaming

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql
import org.scalatest.FunSpec

class KinesisAppTests extends FunSpec with DataFrameSuiteBase {
  describe("Kinesis App Tests") {
    it("should be able to parse json into a dataframe") {
      // Example data to parse
      val data = List(
        "{\"ToCountry\": \"US\", \"ToState\": \"MO\", \"SmsMessageSid\": \"SM52c2a4d44e5490eb9766fabd9e6734d6\", \"NumMedia\": \"0\", \"ToCity\": \"\", \"FromZip\": \"62966\", \"SmsSid\": \"SM52c2a4d44e5490eb9766fabd9e6734d6\", \"FromState\": \"IL\", \"SmsStatus\": \"received\", \"FromCity\": \"MURPHYSBORO\", \"Body\": \"Hello there\", \"FromCountry\": \"US\", \"To\": \"+13143105899\", \"ToZip\": \"\", \"NumSegments\": \"1\", \"MessageSid\": \"SM52c2a4d44e5490eb9766fabd9e6734d6\", \"AccountSid\": \"not_a_real_sid\", \"From\": \"+13145551212\", \"ApiVersion\": \"2010-04-01\", \"insertdt\": \"2019-04-01 15:41:35\"}",
        "{\"ToCountry\": \"US\", \"ToState\": \"MO\", \"SmsMessageSid\": \"SMc453b52badcb09451836d3fcb944cbfa\", \"NumMedia\": \"0\", \"ToCity\": \"\", \"FromZip\": \"20166\", \"SmsSid\": \"SMc453b52badcb09451836d3fcb944cbfa\", \"FromState\": \"VA\", \"SmsStatus\": \"received\", \"FromCity\": \"STERLING\", \"Body\": \"SMS from Python\", \"FromCountry\": \"US\", \"To\": \"+13143105899\", \"ToZip\": \"\", \"NumSegments\": \"1\", \"MessageSid\": \"SMc453b52badcb09451836d3fcb944cbfa\", \"AccountSid\": \"not_a_real_sid\", \"From\": \"+13145551212\", \"ApiVersion\": \"2010-04-01\", \"insertdt\": \"2019-04-01 15:38:55\"}",
        "{\"ToCountry\": \"US\", \"ToState\": \"MO\", \"SmsMessageSid\": \"SM9b78712388f32a36ed9a8143f9afb391\", \"NumMedia\": \"0\", \"ToCity\": \"\", \"FromZip\": \"20166\", \"SmsSid\": \"SM9b78712388f32a36ed9a8143f9afb391\", \"FromState\": \"VA\", \"SmsStatus\": \"received\", \"FromCity\": \"STERLING\", \"Body\": \"How are you?\", \"FromCountry\": \"US\", \"To\": \"+13143105899\", \"ToZip\": \"\", \"NumSegments\": \"1\", \"MessageSid\": \"SM9b78712388f32a36ed9a8143f9afb391\", \"AccountSid\": \"not_a_real_sid\", \"From\": \"+13145551212\", \"ApiVersion\": \"2010-04-01\", \"insertdt\": \"2019-04-01 15:38:51\"}"
      )
      val rdd = sc.parallelize(data)
      val expectedData = List(
        sql.Row("62966", "MURPHYSBORO", "IL", "Hello there"),
        sql.Row("20166", "STERLING", "VA", "SMS from Python"),
        sql.Row("20166", "STERLING", "VA", "How are you?")
      )

      val expectedDf = spark.createDataFrame(sc.parallelize(expectedData), KinesisApp.twilioJsonSchema)
      val actualDf = KinesisApp.jsonToDataframe(spark, rdd)
      assertDataFrameEquals(expectedDf, actualDf)
    }
  }

}
