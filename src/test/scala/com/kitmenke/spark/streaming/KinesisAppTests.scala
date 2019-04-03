package com.kitmenke.spark.streaming

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.FunSpec

class KinesisAppTests extends FunSpec with SharedSparkContext with RDDComparisons {
  describe("Kinesis App Tests") {
    it("should be able to parse json") {
      val data = List(
        "{\"ToCountry\": \"US\", \"ToState\": \"MO\", \"SmsMessageSid\": \"SM52c2a4d44e5490eb9766fabd9e6734d6\", \"NumMedia\": \"0\", \"ToCity\": \"\", \"FromZip\": \"62966\", \"SmsSid\": \"SM52c2a4d44e5490eb9766fabd9e6734d6\", \"FromState\": \"IL\", \"SmsStatus\": \"received\", \"FromCity\": \"MURPHYSBORO\", \"Body\": \"Hello there\", \"FromCountry\": \"US\", \"To\": \"+13143105899\", \"ToZip\": \"\", \"NumSegments\": \"1\", \"MessageSid\": \"SM52c2a4d44e5490eb9766fabd9e6734d6\", \"AccountSid\": \"not_a_real_sid\", \"From\": \"+13145551212\", \"ApiVersion\": \"2010-04-01\", \"insertdt\": \"2019-04-01 15:41:35\"}".getBytes,
        "{\"ToCountry\": \"US\", \"ToState\": \"MO\", \"SmsMessageSid\": \"SMc453b52badcb09451836d3fcb944cbfa\", \"NumMedia\": \"0\", \"ToCity\": \"\", \"FromZip\": \"20166\", \"SmsSid\": \"SMc453b52badcb09451836d3fcb944cbfa\", \"FromState\": \"VA\", \"SmsStatus\": \"received\", \"FromCity\": \"STERLING\", \"Body\": \"SMS from Python\", \"FromCountry\": \"US\", \"To\": \"+13143105899\", \"ToZip\": \"\", \"NumSegments\": \"1\", \"MessageSid\": \"SMc453b52badcb09451836d3fcb944cbfa\", \"AccountSid\": \"not_a_real_sid\", \"From\": \"+13145551212\", \"ApiVersion\": \"2010-04-01\", \"insertdt\": \"2019-04-01 15:38:55\"}".getBytes,
        "{\"ToCountry\": \"US\", \"ToState\": \"MO\", \"SmsMessageSid\": \"SM9b78712388f32a36ed9a8143f9afb391\", \"NumMedia\": \"0\", \"ToCity\": \"\", \"FromZip\": \"20166\", \"SmsSid\": \"SM9b78712388f32a36ed9a8143f9afb391\", \"FromState\": \"VA\", \"SmsStatus\": \"received\", \"FromCity\": \"STERLING\", \"Body\": \"How are you?\", \"FromCountry\": \"US\", \"To\": \"+13143105899\", \"ToZip\": \"\", \"NumSegments\": \"1\", \"MessageSid\": \"SM9b78712388f32a36ed9a8143f9afb391\", \"AccountSid\": \"not_a_real_sid\", \"From\": \"+13145551212\", \"ApiVersion\": \"2010-04-01\", \"insertdt\": \"2019-04-01 15:38:51\"}".getBytes
      )
      val rdd = sc.parallelize(data)
      val actualRdd = KinesisApp.parseJson(rdd)
      actualRdd.collect().foreach(println)
      val expectedData = List(
        TwilioMessage("Hello there", "62966", "MURPHYSBORO", "IL"),
        TwilioMessage("SMS from Python", "20166", "STERLING", "VA"),
        TwilioMessage("How are you?", "20166", "STERLING", "VA")
      )
      val expectedRdd = sc.parallelize(expectedData)
      //assertRDDEquals(expectedRdd, actualRdd)
      assert(None === compareRDD(expectedRdd, actualRdd))
    }

    it("should be able to determine sentiment for some messages") {
      val good = TwilioMessage("Everything is awesome!", "", "", "")
      val bad = TwilioMessage("This is the worst.", "", "", "")
      val data = List(good, bad)
      val rdd = sc.parallelize(data)
      val actualRdd = KinesisApp.addSentiment(rdd)
      val expectedData = List((good, "POSITIVE"), (bad, "NEGATIVE"))
      val expectedRdd = sc.parallelize(expectedData)
      assert(None === compareRDD(expectedRdd, actualRdd))
    }
  }

}
