package com.tobe.examples.openmldb

import java.sql.DriverManager

object ConnectOpenmldbCluster {


  // support table as input
  // support switch to disable
  // Show the service endpoint
  // Export table with endpoint

  // P0 fangan

  def main(argv: Array[String]): Unit = {

    val zkHost = "localhost:2181"
    val zkPath = "/openmldb"
    //val sql = "SELECT 100"
    val sql = "CREATE DATABASE db2"

    Class.forName("com._4paradigm.openmldb.jdbc.SQLDriver")

    System.out.println(s"Try to connect OpenMLDB with zk host: $zkHost, path: $zkPath")
    val connection = DriverManager.getConnection(s"jdbc:openmldb:///?zk=$zkHost&zkPath=$zkPath")
    val stmt = connection.createStatement

    stmt.execute("SET @@execute_mode='online'")
    stmt.execute(sql)

    if (sql.toLowerCase.startsWith("select")) {
      val result = stmt.getResultSet
      while (result.next()) {
        System.out.println(result.getInt(1))
      }
    }

    System.out.println(s"Success to execute SQL: $sql")
  }

}
