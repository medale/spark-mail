package com.uebercomputing.spark.sql

/**
 */
object DataFrameToRdd {

  def main(args: Array[String]): Unit = {
    val sqlContext = org.apache.spark.sql.test.TestSQLContext
    val emailsDf = sqlContext.read.parquet("/opt/rpm1/enron/parquet/out")
    val y2kDf = emailsDf.where(emailsDf("year") === 2000)
    //RDD[org.apache.spark.sql.Row], also .rdd, flatMap, toJSON...
    val mailFieldSizesY2kRdd = y2kDf.map(row => {
      val mailFields = row.getAs[Map[String, String]]("mailFields")
      mailFields.size
    })
    //DoubleRDDFunctions
    mailFieldSizesY2kRdd.stats()
    //(count: 196100, mean: 14.000347, stdev: 0.018618, max: 15.000000, min: 14.000000)
  }

}
