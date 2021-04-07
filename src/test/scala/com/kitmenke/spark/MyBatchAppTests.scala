package com.kitmenke.spark

import org.scalatest.FunSpec

class MyBatchAppTests extends FunSpec {
  describe("word count") {
    it("should be able to split a sentence into words") {
      // given
      val sentence = "one two three"
      val expected = Array("one", "two", "three")
      // when
      val actual = MyBatchApp.splitSentenceIntoWords(sentence)
      // then
      assert(actual === expected)
    }

    it("should make words lowercase and remove punctuation") {
      // given
      val sentence = "Boy, that escalated QUICKLY..."
      val expected = Array("boy", "that", "escalated", "quickly")
      // when
      val actual = MyBatchApp.splitSentenceIntoWords(sentence)
      // then
      assert(actual === expected)
    }
  }
}
