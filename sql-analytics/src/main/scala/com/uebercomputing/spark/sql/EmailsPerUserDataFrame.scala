package com.uebercomputing.spark.sql

import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf

import com.databricks.spark.avro._

/**
 */
object EmailsPerUserDataFrame {

  def main(args: Array[String]): Unit = {
    val sqlContext = org.apache.spark.sql.test.TestSQLContext
    //load file via Databricks' spark-avro library
    val recordsDf = sqlContext.avroFile("enron.avro")

    val getUserUdf = udf((mailFields: Map[String, String]) => mailFields("UserName"))

    //if implicits._ => $ instead of recordsDf("...")
    // SQLContext.implicits.StringToColumn(val sc: StringContext) { def $(
    import sqlContext.implicits._
    val recordsWithUserDf = recordsDf.withColumn("user", getUserUdf($"mailFields"))
    // groupBy - GroupedData.count - adds "count" column to resulting DF
    // DF has user,count
    // $"count" - count column - desc function/order
    recordsWithUserDf.groupBy("user").count().orderBy($"count".desc)
    //Array([kaminski-v,28465], [dasovich-j,28234], [kean-s,25351], [mann-k,23381],
    //[jones-t,19950], [shackleton-s,18687], [taylor-m,13875], [farmer-d,13032],
    //[germany-c,12436], [beck-s,11830])
  }
}
