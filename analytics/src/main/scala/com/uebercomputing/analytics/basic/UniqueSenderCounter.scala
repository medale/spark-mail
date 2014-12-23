package com.uebercomputing.analytics.basic

import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.uebercomputing.mailrecord.MailRecord
import com.uebercomputing.analytics.util.MailMasterOptionParser
import org.apache.avro.mapreduce.AvroJob

/**
 * Run with two args:
 * --avroMailFile /opt/rpm1/enron/enron_mail_20110402/mail.avro --master local[4]
 */
object UniqueSenderCounter extends MailMasterOptionParser {

  def main(args: Array[String]): Unit = {
    val configOpt = config(args)
    configOpt.map { config =>
      println(s"Loading from ${config.avroMailFile} with master ${config.master}...")
      val sparkConf = new SparkConf().setAppName("Unique Senders").setMaster(config.master)
      val sc = new SparkContext(sparkConf)

      val job = Job.getInstance()
      AvroJob.setInputKeySchema(job, MailRecord.getClassSchema)

      //FileInputFormat.setInputPaths(job, config.avroMailFile)
      //      val recordsKeyValues = sc.newAPIHadoopRDD(job.getConfiguration,
      //        classOf[AvroKeyInputFormat[MailRecord]],
      //        classOf[AvroKey[MailRecord]],
      //        classOf[NullWritable])

      val recordsKeyValues = sc.newAPIHadoopFile(config.avroMailFile,
        classOf[AvroKeyInputFormat[MailRecord]],
        classOf[AvroKey[MailRecord]],
        classOf[NullWritable],
        job.getConfiguration)

      val allFroms = recordsKeyValues.map {
        recordKeyValueTuple =>
          val mailRecord = recordKeyValueTuple._1.datum()
          (mailRecord.getFrom)
      }
      val allFromsCount = allFroms.count()
      val uniqueFromsCount = allFroms.distinct().count()
      println(s"All froms were $allFromsCount, unique froms were $uniqueFromsCount")
    }
  }
}
