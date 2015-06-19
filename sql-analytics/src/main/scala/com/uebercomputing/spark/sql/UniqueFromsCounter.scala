package com.uebercomputing.spark.sql

import org.apache.spark.sql._
import com.databricks.spark.avro._

object UniqueFromsCounter {

  def main(args: Array[String]): Unit = {
    //val sparkConf = new SparkConf().setAppName("MailRecord").setMaster("local")
    //val sc = new SparkContext(sparkConf)
    //val sqlContext = new SQLContext(sc)
    val sqlContext = org.apache.spark.sql.test.TestSQLContext
    //assumes either we are using HDFS with enron.avro in user's home directory
    //or local file system with enron.avro a link to the actual file with the
    //link in the directory where we started spark from.

    val recordsDf = sqlContext.avroFile("enron.avro")

    val uniqueFroms = recordsDf.select("from").distinct.count()
    println(uniqueFroms)
  }
}
