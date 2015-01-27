package com.uebercomputing.mailrecord

import org.apache.spark.SparkConf

/**
 * When running spark-submit or spark-shell from command line add:
 *
 * --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
 * --conf spark.kryo.registrator=com.uebercomputing.mailrecord.MailRecordRegistrator \
 * --conf spark.kryoserializer.buffer.mb=128 \
 * --conf spark.kryoserializer.buffer.max.mb=512
 */
object MailRecordSparkConfFactory {

  //keys from actual SparkConf.scala file setMaster/setAppName methods
  val MasterKey = "spark.master"
  val SerializerKey = "spark.serializer"
  val KryoSerializer = "org.apache.spark.serializer.KryoSerializer"

  val KryoRegistratorKey = "spark.kryo.registrator"

  //initial size of Kryo buffer for serialization - default 2MB
  val KryoBufferSizeMBKey = "spark.kryoserializer.buffer.mb"
  val MailRecordBufferSize = "128" //in MB
  //max limit - default 64MB - increase if error Kryo buffer limit exceeded
  val KryoBufferSizeMaxMBKey = "spark.kryoserializer.buffer.max.mb"
  val MailRecordBufferSizeMax = "512" //in MB

  def apply(appName: String, props: Map[String, String]): SparkConf = {
    val conf = new SparkConf().setAppName(appName)
    for ((key, value) <- props) {
      conf.set(key, value)
    }
    conf.set(SerializerKey, KryoSerializer)
    conf.set(KryoRegistratorKey, "com.uebercomputing.mailrecord.MailRecordRegistrar")
    conf.set(KryoBufferSizeMBKey, MailRecordBufferSize)
    conf.set(KryoBufferSizeMaxMBKey, MailRecordBufferSizeMax)
    conf
  }
}
