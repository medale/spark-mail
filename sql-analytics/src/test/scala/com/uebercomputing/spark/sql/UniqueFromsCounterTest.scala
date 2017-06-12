package com.uebercomputing.spark.sql

import com.databricks.spark.avro._
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.scalatest.FunSuite

class UniqueFromsCounterTest extends FunSuite with SharedSparkContext with DataFrameSuiteBase {

  test("Mail records") {
    val sqlCtx = sqlContext
    // import com.databricks.spark.avro._
    val recordsDf = sqlContext.read.avro("enron.avro")
    val uniqueFroms = recordsDf.select("from").distinct.count()
    println(uniqueFroms)
  }
}
