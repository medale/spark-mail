package com.uebercomputing.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.numericRDDToDoubleRDDFunctions
import org.apache.spark.sql.SQLContext

/**
 */
object DataFrameToRdd {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("test")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val emailsDf = sqlContext.read.parquet("/opt/rpm1/enron/parquet/out")
    val y2kDf = emailsDf.where(emailsDf("year") === 2000)
    // RDD[org.apache.spark.sql.Row], also .rdd, flatMap, toJSON...
    import sqlContext.implicits._
    val mailFieldSizesY2kRdd = y2kDf.map(row => {
      val mailFields = row.getAs[Map[String, String]]("mailFields")
      mailFields.size
    }).rdd
    // DoubleRDDFunctions
    mailFieldSizesY2kRdd.stats()
    // (count: 196100, mean: 14.000347, stdev: 0.018618, max: 15.000000, min: 14.000000)
  }

}
