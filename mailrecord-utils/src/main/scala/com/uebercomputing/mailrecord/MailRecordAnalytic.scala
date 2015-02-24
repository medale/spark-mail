package com.uebercomputing.mailrecord

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.log4j.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.avro.mapred.AvroKey
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.avro.mapreduce.AvroJob

case class AnalyticInput(val sc: SparkContext, val mailRecordsRdd: RDD[MailRecord], hadoopConfig: Configuration, config: Config) {}

object MailRecordAnalytic {

  def getAnalyticInput(appName: String, args: Array[String], additionalSparkProps: Map[String, String], logger: Logger): AnalyticInput = {
    val configOpt = CommandLineOptionsParser.getConfigOpt(args)
    configOpt match {
      case Some(config) => {
        val sparkConf = MailRecordSparkConfFactory(appName, additionalSparkProps)
        config.masterOpt.foreach { master => sparkConf.setMaster(master) }
        val sc = new SparkContext(sparkConf)
        val (mailRecordsAvroRdd, hadoopConfig) = getAvroRddHadoopConfigTuple(sc, config)
        val mailRecordsRdd = getMailRecordsRdd(mailRecordsAvroRdd)
        AnalyticInput(sc, mailRecordsRdd, hadoopConfig, config)
      }
      case None => {
        val errMsg = s"Unable to process command line options."
        logger.error(errMsg)
        throw new RuntimeException(errMsg)
      }
    }
  }

  /**
   * Convenience method when running from Spark shell (using
   * CommandLineOptionsParser.getConfigOpt(args) to obtain config).
   */
  def getMailRecordsRdd(sc: SparkContext, config: Config): RDD[MailRecord] = {
    val (mailRecordAvroRdd, job) = getAvroRddHadoopConfigTuple(sc, config)
    getMailRecordsRdd(mailRecordAvroRdd)
  }

  //TODO: Create cacheable and nonCacheable versions of getMailRecordsRdd

  /**
   * RDD with just the mail record objects. This version is cacheable but does create more
   * objects as it needs to make a copy of the original MailRecord that is being reused.
   */
  def getMailRecordsRdd(mailRecordsAvroRdd: RDD[(AvroKey[MailRecord], FileSplit)]): RDD[MailRecord] = {
    val mailRecordsRdd = mailRecordsAvroRdd.map {
      case (mailRecordAvroKey, fileSplit) =>
        val mailRecord = mailRecordAvroKey.datum()
        //make a copy - MailRecord gets reused
        MailRecord.newBuilder(mailRecord).build()
    }
    mailRecordsRdd
  }

  /**
   * Uses MailRecordInputFormat based on config to create an RDD of AvroKey[MailRecord] and FileSplit
   * tuples (FileSplit pinpoints which file/offset the data came from).
   */
  def getAvroRddHadoopConfigTuple(sc: SparkContext, config: Config): (RDD[(AvroKey[MailRecord], FileSplit)], Configuration) = {
    val hadoopConf = getHadoopConfiguration(config)
    val sparkHadoopConf = sc.hadoopConfiguration
    hadoopConf.addResource(sparkHadoopConf)
    hadoopConf.setBoolean(FileInputFormat.INPUT_DIR_RECURSIVE, true)
    val mailRecordsAvroRdd = sc.newAPIHadoopFile(config.avroMailInput,
      classOf[MailRecordInputFormat], classOf[AvroKey[MailRecord]], classOf[FileSplit], hadoopConf)
    (mailRecordsAvroRdd, hadoopConf)
  }

  def getHadoopConfiguration(config: Config): Configuration = {
    val hadoopConf = config.hadoopConfPathOpt match {
      case Some(hadoopConfPath) => {
        val conf = new Configuration()
        conf.addResource(hadoopConfPath)
        conf
      }
      case None => new Configuration()
    }
    hadoopConf
  }
}
