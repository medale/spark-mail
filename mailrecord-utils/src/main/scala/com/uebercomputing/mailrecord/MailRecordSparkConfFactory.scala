package com.uebercomputing.mailrecord

import org.apache.spark.SparkConf

/**
 * When running spark-submit or spark-shell from command line add:
 *
 * --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.kryo.registrator=com.uebercomputing.mailrecord.MailRecordRegistrator --conf spark.kryoserializer.buffer.mb=512
 */
object MailRecordSparkConfFactory {

  val appNameKey = "app.name"
  val masterKey = "master"
  val SerializerKey = "spark.serializer"
  val KryoSerializer = "org.apache.spark.serializer.KryoSerializer"

  val KryoRegistratorKey = "spark.kryo.registrator"

  //must set to size of largest object to be serialized - default 2MB
  val KryoBufferSizeMBKey = "spark.kryoserializer.buffer.mb"
  val MailRecordBufferSize = "512" //in MB

  def apply(props: Map[String, String]): SparkConf = {
    val conf = new SparkConf()
    props.get(appNameKey).foreach { name => conf.setAppName(name) }
    props.get(masterKey).foreach { master => conf.setMaster(master) }
    conf.set(SerializerKey, KryoSerializer)
    conf.set(KryoRegistratorKey, "com.uebercomputing.mailrecord.MailRecordRegistrator")
    conf.set(KryoBufferSizeMBKey, MailRecordBufferSize)
    conf
  }
}
