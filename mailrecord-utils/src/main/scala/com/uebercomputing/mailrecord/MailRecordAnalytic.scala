package com.uebercomputing.mailrecord

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.log4j.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.avro.mapred.AvroKey
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.Path

case class AnalyticInput(val sc: SparkContext, val mailRecordRdd: RDD[MailRecord], config: Config) {}

trait MailRecordAnalytic extends CommandLineOptionsParser {

  def getAnalyticInput(appName: String, args: Array[String], additionalSparkProps: Map[String, String], logger: Logger): AnalyticInput = {
    val configOpt = getConfigOpt(args)
    configOpt match {
      case Some(config) => {
        val sparkConf = MailRecordSparkConfFactory(appName, additionalSparkProps)
        config.masterOpt.foreach { master => sparkConf.setMaster(master) }
        val sc = new SparkContext(sparkConf)
        val mailRecordRdd = getMailRecordRdd(sc, config)
        AnalyticInput(sc, mailRecordRdd, config)
      }
      case None => {
        val errMsg = s"Unable to process command line options."
        logger.error(errMsg)
        throw new RuntimeException(errMsg)
      }
    }
  }

  /**
   * RDD with just the mail record objects.
   */
  def getMailRecordRdd(sc: SparkContext, config: Config): RDD[MailRecord] = {
    val mailRecordAvroRdd = getRddFromMailRecordInputFormat(sc, config)
    val mailRecordRdd = mailRecordAvroRdd.map { avroKeySplitTuple =>
      val (mailRecordAvroKey, fileSplit) = avroKeySplitTuple
      mailRecordAvroKey.datum()
    }
    mailRecordRdd
  }

  /**
   * For analytics that need to know the file split where a record came from.
   */
  def getMailRecordFileSplitTupleRdd(sc: SparkContext, config: Config): RDD[(MailRecord, FileSplit)] = {
    val mailRecordAvroRdd = getRddFromMailRecordInputFormat(sc, config)
    val mailRecordFileSplitTupleRdd = mailRecordAvroRdd.map { avroKeySplitTuple =>
      val (mailRecordAvroKey, fileSplit) = avroKeySplitTuple
      (mailRecordAvroKey.datum(), fileSplit)
    }
    mailRecordFileSplitTupleRdd
  }

  def getRddFromMailRecordInputFormat(sc: SparkContext, config: Config): RDD[(AvroKey[MailRecord], FileSplit)] = {
    val hadoopConf = config.hadoopConfPathOpt match {
      case Some(hadoopConfPath) => {
        val conf = new Configuration()
        conf.addResource(hadoopConfPath)
        conf
      }
      case None => getLocalHadoopConf()
    }
    val job = Job.getInstance(hadoopConf)
    val path = new Path(config.avroMailInput)
    MailRecordInputFormat.addInputPath(job, path)
    MailRecordInputFormat.setInputDirRecursive(job, true)
    //Note: addInputPath makes clone of configuration and adds input path
    //to that copy. Therefore must call job.getConfiguration!
    val mailRecordAvroRdd = sc.newAPIHadoopRDD(job.getConfiguration,
      classOf[MailRecordInputFormat], classOf[AvroKey[MailRecord]], classOf[FileSplit])

    mailRecordAvroRdd
  }
}
