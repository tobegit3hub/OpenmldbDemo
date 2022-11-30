package com.tobe.examples.oss

import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object WriteOssParquet {

  def main(argv: Array[String]): Unit = {
    val ak = "foo"
    val sk = "bar"

    val spark = SparkSession.builder()
      .master("local")
      .config("spark.hadoop.fs.oss.impl", "com.aliyun.fs.oss.nat.NativeOssFileSystem")
      .config("spark.hadoop.fs.oss.accessKeyId", ak)
      .config("spark.hadoop.fs.oss.accessKeySecret", sk)
      .config("spark.hadoop.mapreduce.job.run-local", true)
      .config("fs.oss.buffer.dirs", "/tmp")
      .getOrCreate()

    val data = Seq(
      Row(1, 1L),
      Row(2, 8L))
    val schema = StructType(List(
      StructField("col1", IntegerType),
      StructField("col2", LongType)))
    val df = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)

    val ossEndpoint = "oss-cn-shenzhen.aliyuncs.com"
    val bucketName = "tobe-bucket"
    val outputPath = "/two_column_table"
    val writeCsvParquetPath = s"oss://$bucketName.$ossEndpoint$outputPath"
    df.write.mode(SaveMode.Overwrite).parquet(writeCsvParquetPath)
  }

}
