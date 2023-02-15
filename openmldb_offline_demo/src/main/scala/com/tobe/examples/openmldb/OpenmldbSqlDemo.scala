package com.tobe.examples.openmldb

import com._4paradigm.openmldb.batch.api.OpenmldbSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object OpenmldbSqlDemo {

  def main(argv: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext
    val sess = new OpenmldbSession(spark)

    val data = Seq(
      Row("A", 10, 112233),
      Row("B", 20, 223311),
      Row("C", 30, 331122))

    val schema = StructType(List(
      StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("phone", IntegerType)))

    val df = spark.createDataFrame(sc.makeRDD(data), schema)
    sess.registerTable("t1", df)

    val outputDf = sess.sql("SELECT age + 1 as new_age FROM t1")
    outputDf.show()
  }

}
