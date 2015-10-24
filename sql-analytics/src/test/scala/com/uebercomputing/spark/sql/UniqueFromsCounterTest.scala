package com.uebercomputing.spark.sql

import org.scalatest.FunSuite
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.test._
import com.databricks.spark.avro._
import com.holdenkarau.spark.testing.SharedSparkContext
import com.holdenkarau.spark.testing.DataFrameSuiteBase

class UniqueFromsCounterTest extends FunSuite with SharedSparkContext with DataFrameSuiteBase {

  test("Mail records") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    //import com.databricks.spark.avro._
    val recordsDf = sqlContext.avroFile("enron.avro")
    val uniqueFroms = recordsDf.select("from").distinct.count()
    println(uniqueFroms)
  }
}
