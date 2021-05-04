package com.kitmenke.spark.integration

import com.dimafeng.testcontainers.GenericContainer
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.testcontainers.containers.wait.strategy.Wait

class HBaseContainer(underlying: GenericContainer) extends GenericContainer(underlying) {
  def initConnection(): Connection = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", s"${this.host}:2181")
    ConnectionFactory.createConnection(conf)
  }
}

object HBaseContainer {
  case class Def() extends GenericContainer.Def[HBaseContainer](
    new HBaseContainer(GenericContainer(
      dockerImage = "harisekhon/hbase:2.1",
      exposedPorts = Seq(2181, 8080, 8085, 9090, 9095, 16000, 16010, 16201, 16301),
      waitStrategy = Wait.forHttp("http://localhost:16010/master-status")
    ))
  )
}