package com.uebercomputing.mailrecord

import org.apache.spark.SparkConf

/**
 * When running spark-submit or spark-shell from command line add:
 *
 * --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.kryo.registrator=com.uebercomputing.mailrecord.MailRecordRegistrator --conf spark.kryoserializer.buffer.mb=512
 */
object MailRecordSparkConfFactory {

  //keys from actual SparkConf.scala file setMaster/setAppName methods
  val AppNameKey = "spark.app.name"
  val MasterKey = "spark.master"
  val SerializerKey = "spark.serializer"
  val KryoSerializer = "org.apache.spark.serializer.KryoSerializer"

  val KryoRegistratorKey = "spark.kryo.registrator"

  //must set to size of largest object to be serialized - default 2MB
  val KryoBufferSizeMBKey = "spark.kryoserializer.buffer.mb"
  val MailRecordBufferSize = "512" //in MB

  def apply(props: Map[String, String]): SparkConf = {
    val conf = new SparkConf()
    for ((key, value) <- props) {
      conf.set(key, value)
    }
    conf.set(SerializerKey, KryoSerializer)
    conf.set(KryoRegistratorKey, "com.uebercomputing.mailrecord.MailRecordRegistrar")
    conf.set(KryoBufferSizeMBKey, MailRecordBufferSize)
    conf
  }
}
