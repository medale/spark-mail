package com.uebercomputing.spark.sql

import org.apache.spark.rdd.RDD.numericRDDToDoubleRDDFunctions
import org.apache.spark.sql.SparkSession

/**
 */
object DataFrameToRdd {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[2]").getOrCreate()
    val emailsDf = spark.read.parquet("/datasets/enron/parquet/out")
    val y2kDf = emailsDf.where(emailsDf("year") === 2000)
    // RDD[org.apache.spark.sql.Row], also .rdd, flatMap, toJSON...
    import spark.implicits._
    val mailFieldSizesY2kRdd = y2kDf
      .map(row => {
        val mailFields = row.getAs[Map[String, String]]("mailFields")
        mailFields.size
      })
      .rdd
    // DoubleRDDFunctions
    mailFieldSizesY2kRdd.stats()
    // (count: 196100, mean: 14.000347, stdev: 0.018618, max: 15.000000, min: 14.000000)
  }

}
