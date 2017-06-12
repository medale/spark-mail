package com.uebercomputing.spark.sql

import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.databricks.spark.avro._

object UniqueFromsCounter {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("test")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    // assumes either we are using HDFS with enron.avro in user's home directory
    // or local file system with enron.avro a link to the actual file with the
    // link in the directory where we started spark from.

    val recordsDf = sqlContext.read.avro("enron.avro")

    val uniqueFroms = recordsDf.select("from").distinct.count()
    println(uniqueFroms)
  }
}
