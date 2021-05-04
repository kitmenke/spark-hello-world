package com.kitmenke.spark.integration

import com.dimafeng.testcontainers.CassandraContainer
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, Connection, TableDescriptorBuilder}
import org.apache.hadoop.hbase.{NamespaceDescriptor, TableName}
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class HBaseIntegrationTests extends FunSpec
  with TestContainerForAll
  with BeforeAndAfterAll {
  override val containerDef = HBaseContainer.Def()
  var connection: Connection = null

  override def afterContainersStart(container: Containers): Unit = {
    super.afterContainersStart(container)

    container match {
      case _: HBaseContainer => {
        connection = container.initConnection()
        val admin = connection.getAdmin()
        admin.createNamespace(NamespaceDescriptor.create("test").build())
        val table = TableDescriptorBuilder.newBuilder(TableName.valueOf("users"))
        table.setColumnFamily(ColumnFamilyDescriptorBuilder.of("f1"))
        admin.createTable(table.build())
      }
    }
  }

  override def beforeContainersStop(container: Containers): Unit = {
    super.beforeContainersStop(container)

    container match {
      case _: HBaseContainer => if (connection != null) connection.close()
    }
  }

  describe("cassandra integration tests") {
    it("should have a keyspace and table") {
      val filteredTables = connection.getAdmin().listTableNames().filter(t => t.getNameAsString == "users")
      assert(1 === filteredTables.size)
    }

  }
}