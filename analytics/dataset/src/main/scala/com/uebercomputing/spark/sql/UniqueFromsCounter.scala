package com.uebercomputing.spark.sql

import org.apache.spark.sql._

object UniqueFromsCounter {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[2]").getOrCreate()

    // assumes either we are using HDFS with enron.avro in user's home directory
    // or local file system with enron.avro a link to the actual file with the
    // link in the directory where we started spark from.

    val recordsDf = spark.read.format("avro").load("enron.avro")

    val uniqueFroms = recordsDf.select("from").distinct.count()
    println(uniqueFroms)
  }

}
