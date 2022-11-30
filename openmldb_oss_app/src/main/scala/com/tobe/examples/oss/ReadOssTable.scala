package com.tobe.examples.oss

import org.apache.spark.sql.SparkSession

object ReadOssTable {

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

    val ossEndpoint = "oss-cn-shenzhen.aliyuncs.com"
    val bucketName = "tobe-bucket"
    val filePath = "/table.csv"
    val ossFilePath = s"oss://$bucketName.$ossEndpoint$filePath"
    val df = spark.read.csv(ossFilePath)
    df.show()
  }

}
