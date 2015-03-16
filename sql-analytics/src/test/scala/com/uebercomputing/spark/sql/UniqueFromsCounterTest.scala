package com.uebercomputing.spark.sql

import org.scalatest.FunSuite
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.test._
import com.databricks.spark.avro._

class UniqueFromsCounterTest extends FunSuite {

  ignore("Mail records") {
    val sqlContext = TestSQLContext
    //import com.databricks.spark.avro._
    val recordsDf = sqlContext.avroFile("enron.avro")
    val uniqueFroms = recordsDf.select("from").distinct.count()
    println(uniqueFroms)
  }
}
