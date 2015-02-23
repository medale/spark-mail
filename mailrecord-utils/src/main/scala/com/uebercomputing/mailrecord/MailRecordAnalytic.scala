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

case class AnalyticInput(val sc: SparkContext, val mailRecordsRdd: RDD[MailRecord], job: Job, config: Config) {}

object MailRecordAnalytic {

  def getAnalyticInput(appName: String, args: Array[String], additionalSparkProps: Map[String, String], logger: Logger): AnalyticInput = {
    val configOpt = CommandLineOptionsParser.getConfigOpt(args)
    configOpt match {
      case Some(config) => {
        val sparkConf = MailRecordSparkConfFactory(appName, additionalSparkProps)
        config.masterOpt.foreach { master => sparkConf.setMaster(master) }
        val sc = new SparkContext(sparkConf)
        val (mailRecordsAvroRdd, job) = getAvroRddJobTuple(sc, config)
        val mailRecordsRdd = getMailRecordsRdd(mailRecordsAvroRdd)
        AnalyticInput(sc, mailRecordsRdd, job, config)
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
    val (mailRecordAvroRdd, job) = getAvroRddJobTuple(sc, config)
    getMailRecordsRdd(mailRecordAvroRdd)
  }

  /**
   * RDD with just the mail record objects.
   */
  def getMailRecordsRdd(mailRecordsAvroRdd: RDD[(AvroKey[MailRecord], NullWritable)]): RDD[MailRecord] = {
    val mailRecordsRdd = mailRecordsAvroRdd.map { avroKeySplitTuple =>
      val (mailRecordAvroKey, fileSplit) = avroKeySplitTuple
      mailRecordAvroKey.datum()
    }
    mailRecordsRdd
  }

  /**
   * Uses MailRecordInputFormat based on config to create an RDD of AvroKey[MailRecord] and NullWritable
   * tuples.
   */
  def getAvroRddJobTuple(sc: SparkContext, config: Config): (RDD[(AvroKey[MailRecord], NullWritable)], Job) = {
    val hadoopConf = config.hadoopConfPathOpt match {
      case Some(hadoopConfPath) => {
        val conf = new Configuration()
        conf.addResource(hadoopConfPath)
        conf
      }
      case None => new Configuration()
    }

    val sparkHadoopConf = sc.hadoopConfiguration
    hadoopConf.addResource(sparkHadoopConf)

    val job = Job.getInstance(hadoopConf)
    val path = new Path(config.avroMailInput)
    MailRecordInputFormat.addInputPath(job, path)
    MailRecordInputFormat.setInputDirRecursive(job, true)
    //Note: addInputPath makes clone of configuration and adds input path
    //to that copy. Therefore must call job.getConfiguration!
    val mailRecordsAvroRdd = sc.newAPIHadoopRDD(job.getConfiguration,
      classOf[MailRecordInputFormat], classOf[AvroKey[MailRecord]], classOf[NullWritable])

    (mailRecordsAvroRdd, job)
  }
}
