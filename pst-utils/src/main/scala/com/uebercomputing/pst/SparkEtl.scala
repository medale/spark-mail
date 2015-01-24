package com.uebercomputing.pst

import com.uebercomputing.mailrecord.MailRecordSparkConfFactory
import org.apache.spark.SparkContext

object SparkEtl {

  def main(args: Array[String]): Unit = {
    val props = Map[String, String]()
    val sparkConf = MailRecordSparkConfFactory(props)
    val sc = new SparkContext(sparkConf)
    //TODO - distributed PST to avro processing...
  }
}
