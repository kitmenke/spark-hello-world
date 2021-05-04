package com.kitmenke.spark.integration

import com.datastax.driver.core.Session
import com.dimafeng.testcontainers.CassandraContainer
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructType}
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class CassandraIntegrationTests extends FunSpec
  with DataFrameSuiteBase
  with TestContainerForAll
  with BeforeAndAfterAll {
  override val containerDef = CassandraContainer.Def()
  var session: Session = null

  override def afterContainersStart(container: Containers): Unit = {
    super.afterContainersStart(container)

    container match {
      case _: CassandraContainer => {
        val cluster = container.cluster
        session = cluster.connect()
        session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};")
        session.execute("CREATE TABLE IF NOT EXISTS test.user (id text PRIMARY KEY, name text, email text);")
        session.execute("INSERT INTO test.user (id, name, email) VALUES ('AAA', 'Jean-Luc Picard', 'jpicard@example.com');")
        session.execute("INSERT INTO test.user (id, name, email) VALUES ('BBB', 'William Riker', 'wriker@example.com');")
        session.execute("INSERT INTO test.user (id, name, email) VALUES ('CCC', 'Deanna Troi', 'dtroi@example.com');")
      }
    }
  }

  override def beforeContainersStop(container: Containers): Unit = {
    super.beforeContainersStop(container)

    container match {
      case _: CassandraContainer => if (session != null) session.close()
    }
  }

  describe("cassandra integration tests") {
    it("should have a keyspace and table") {
      import scala.collection.JavaConverters._
      val keyspaces = session.getCluster.getMetadata.getKeyspaces
      val filteredKeyspaces = keyspaces.asScala.filter(km => km.getName.equals("test"))
      assert(1 === filteredKeyspaces.size)
      val tables = filteredKeyspaces.head.getTables.asScala
      val table = tables.filter(t => t.getName.equals("user"))
      assert(1 === table.size)
    }

    it("can enrich a dataframe with data from cassandra") {
      withContainers { container =>
        // create some sample data to run through our program
        val rdd = sc.parallelize(Seq(
          Row("AAA", 1)
        ))
        val df = sqlContext.createDataFrame(rdd, LookupApp.userSchema)
        val ds = LookupApp.enrichFromDatabase(spark, df, container.host)
        ds.show()
        val enrichedUser = ds.first()
        assert(enrichedUser.name === "Jean-Luc Picard")
      }
    }
  }
}