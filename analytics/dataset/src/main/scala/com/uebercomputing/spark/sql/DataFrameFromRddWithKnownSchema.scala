package com.uebercomputing.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 */
object DataFrameFromRddWithKnownSchema {

  case class Role(emailPrefix: String, name: String, position: String, location: String)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Rdd with known schema").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // convert RDD to DataFrame - rddToDataFrameHolder
    import sqlContext.implicits._

    val rolesRdd = sc.textFile("roles.csv")
    val rolesDf = rolesRdd.map(s => s.split(",")).
      map(lineArray => Role(lineArray(0), lineArray(1), lineArray(2), lineArray(3))).toDF()

  }
}
