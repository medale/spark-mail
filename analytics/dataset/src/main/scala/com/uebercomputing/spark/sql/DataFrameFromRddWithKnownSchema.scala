package com.uebercomputing.spark.sql

import org.apache.spark.sql.SparkSession

/**
 */
object DataFrameFromRddWithKnownSchema {

  case class Role(emailPrefix: String, name: String, position: String, location: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().
      appName("test").
      master("local[2]").
      getOrCreate()

    // convert RDD to DataFrame - rddToDataFrameHolder
    import spark.implicits._

    val rolesRdd = spark.sparkContext.textFile("roles.csv")
    val rolesDf = rolesRdd.map(s => s.split(",")).
      map(lineArray => Role(lineArray(0), lineArray(1), lineArray(2), lineArray(3))).toDF()

  }
}
