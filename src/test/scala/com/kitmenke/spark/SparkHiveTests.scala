package com.kitmenke.spark

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.hive.HiveContext
import org.scalatest.FunSpec

class SparkHiveTests extends FunSpec with SharedSparkContext {
  describe("spark and hive tests") {
    it("should be able to load and query a hive table") {
      // TODO: set this up before running all your tests
      val sqlContext = new HiveContext(sc)
      // default directory is /user/hive/warehouse which doesn't exist on my local machine so use a different dir
      sqlContext.setConf("hive.metastore.warehouse.dir", "/tmp/hive/warehouse")
      // data will persist in this directory so drop the table to get a clean run if it exists
      sqlContext.sql("DROP TABLE IF EXISTS src")
      sqlContext.sql("CREATE TABLE src (key INT, value STRING)")
      sqlContext.sql("LOAD DATA LOCAL INPATH 'src/test/resources/kv1.txt' INTO TABLE src")
      val result = sqlContext.sql("SELECT count(*) FROM src").collect().head
      assert(result(0) === 3l)
    }
  }
}
