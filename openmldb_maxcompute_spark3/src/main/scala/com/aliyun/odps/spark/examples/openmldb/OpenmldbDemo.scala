package com.aliyun.odps.spark.examples.openmldb

import com._4paradigm.openmldb.batch.api.OpenmldbSession
import org.apache.spark.sql.SparkSession

object OpenmldbDemo {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("OpenmldbDemo")
      .getOrCreate()

    val sess = new OpenmldbSession(spark)
    val sql = "SELECT 1"
    sess.sql(sql).show()

    spark.close()
  }
}
