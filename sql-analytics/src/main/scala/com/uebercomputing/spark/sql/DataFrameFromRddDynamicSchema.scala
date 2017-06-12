package com.uebercomputing.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row

/**
 */
object DataFrameFromRddDynamicSchema {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Rdd with known schema").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // convert RDD to DataFrame - rddToDataFrameHolder
    import sqlContext.implicits._

    val rolesRdd = sc.textFile("roles.csv")

    val types = List(("emailPrefix", StringType),
      ("name", StringType), ("position", StringType),
      ("location", StringType))

    val fields = types.map {
      case (name, structType) =>
        StructField(name, structType, nullable = false)
    }

    val schema = StructType(fields)

    val rolesRowRdd = rolesRdd.map(s => s.split(",")).
      map(lineArray => Row(lineArray(0), lineArray(1), lineArray(2), lineArray(3)))

    val rolesDf = sqlContext.createDataFrame(rolesRowRdd, schema)
  }
}
