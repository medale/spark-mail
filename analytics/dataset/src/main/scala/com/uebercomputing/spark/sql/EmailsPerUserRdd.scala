package com.uebercomputing.spark.sql

import org.apache.spark.rdd._
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.hadoop.io.NullWritable
import com.uebercomputing.mailrecord._
import com.uebercomputing.mailrecord.Implicits.mailRecordToMailRecordOps
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.uebercomputing.mailparser.enronfiles.MessageProcessor

/**
 */
object EmailsPerUserRdd {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Emails per user").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val hadoopConf = sc.hadoopConfiguration

    val mailRecordsAvroRdd =
      sc.newAPIHadoopFile("enron.avro",
        classOf[AvroKeyInputFormat[MailRecord]],
        classOf[AvroKey[MailRecord]],
        classOf[NullWritable], hadoopConf)

    val recordsRdd = mailRecordsAvroRdd.map {
      case (avroKey, _) => avroKey.datum()
    }

    val emailsPerUserRdd =
      recordsRdd.flatMap(mailRecord => {
        val userNameOpt =
          mailRecord.getMailFieldOpt(
            MessageProcessor.UserName)
        if (userNameOpt.isDefined) Some(userNameOpt.get, 1)
        else None
      }).reduceByKey(_ + _).sortBy(((t: (String, Int)) => t._2), ascending = false)
    emailsPerUserRdd.take(10)
    //res1: Array[(String, Int)] = Array((kaminski-v,28465), (dasovich-j,28234),
    //(kean-s,25351), (mann-k,23381), (jones-t,19950), (shackleton-s,18687),
    //(taylor-m,13875), (farmer-d,13032), (germany-c,12436), (beck-s,11830))
  }
}
