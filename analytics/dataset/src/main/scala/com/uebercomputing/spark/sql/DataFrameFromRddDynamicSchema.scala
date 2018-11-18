package com.uebercomputing.spark.sql

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/**
 */
object DataFrameFromRddDynamicSchema {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().
      appName("test").
      master("local[2]").
      getOrCreate()

    // convert RDD to DataFrame - rddToDataFrameHolder
    import spark.implicits._

    val rolesRdd = spark.sparkContext.textFile("roles.csv")

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

    val rolesDf = spark.createDataFrame(rolesRowRdd, schema)
  }
}
