package com.aliyun.odps.spark.examples.openmldb.demo

import com._4paradigm.openmldb.batch.api.OpenmldbSession
import org.apache.spark.sql.SparkSession

/**
 * spark-submit --master yarn --deploy-mode cluster --class com.aliyun.odps.spark.examples.openmldb.demo.OpenmldbMaxcomputeTaxiDemo ./target/spark-examples_2.12-1.0.0-SNAPSHOT-shaded.jar
 */
object OpenmldbMaxcomputeTaxiDemo {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("OpenmldbExportMaxcomputeTable")
      .config("spark.sql.defaultCatalog","odps")
      .config("spark.sql.catalog.odps", "org.apache.spark.sql.execution.datasources.v2.odps.OdpsTableCatalog")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("spark.sql.extensions", "org.apache.spark.sql.execution.datasources.v2.odps.extension.OdpsExtensions")
      .config("spark.sql.catalogImplementation","hive")
      .getOrCreate()

    val sess = new OpenmldbSession(spark)

    // Read MaxCompute input table
    val inputTableName = "taxi_tour_table"
    sess.registerTable("t1", spark.table(inputTableName))

    val sql =
      """
        |SELECT trip_duration, passenger_count,
        |sum(pickup_latitude) OVER w AS vendor_sum_pl,
        |max(pickup_latitude) OVER w AS vendor_max_pl,
        |min(pickup_latitude) OVER w AS vendor_min_pl,
        |avg(pickup_latitude) OVER w AS vendor_avg_pl,
        |sum(pickup_latitude) OVER w2 AS pc_sum_pl,
        |max(pickup_latitude) OVER w2 AS pc_max_pl,
        |min(pickup_latitude) OVER w2 AS pc_min_pl,
        |avg(pickup_latitude) OVER w2 AS pc_avg_pl,
        |count(vendor_id) OVER w2 AS pc_cnt,
        |count(vendor_id) OVER w AS vendor_cnt
        |FROM t1
        |WINDOW w AS (PARTITION BY vendor_id ORDER BY pickup_datetime ROWS_RANGE BETWEEN 1d PRECEDING AND CURRENT ROW),
        |w2 AS (PARTITION BY passenger_count ORDER BY pickup_datetime ROWS_RANGE BETWEEN 1d PRECEDING AND CURRENT ROW)
        |""".stripMargin

    // Run OpenMLDB offline execution
    val outputDf = sess.sql(sql).getSparkDf()

    // Create MaxCompute output table
    val outputTableName = "taxi_tour_fe_output"
    spark.sql(s"DROP TABLE IF EXISTS $outputTableName")
    val createTableSql =
      s"""
        |CREATE TABLE $outputTableName (
        |trip_duration INT,
        |passenger_count INT,
        |vendor_sum_pl DOUBLE,
        |vendor_max_pl DOUBLE,
        |vendor_min_pl DOUBLE,
        |vendor_avg_pl DOUBLE,
        |pc_sum_pl DOUBLE,
        |pc_max_pl DOUBLE,
        |pc_min_pl DOUBLE,
        |pc_avg_pl DOUBLE,
        |pc_cnt INT,
        |vendor_cnt INT
        |)
        |""".stripMargin
    spark.sql(createTableSql)

    // Save execution result to output table
    outputDf.writeTo(outputTableName).overwritePartitions()

    sess.close()
  }
}
