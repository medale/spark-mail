package com.uebercomputing.analytics.basic

import org.apache.log4j.Logger
import org.apache.spark.SparkContext.numericRDDToDoubleRDDFunctions
import com.uebercomputing.mailrecord.ExecutionTimer
import com.uebercomputing.mailrecord.Implicits.mailRecordToMailRecordOps
import com.uebercomputing.mailrecord.MailRecordAnalytic
import org.apache.spark.rdd.RDD

/**
 * Run with two args:
 *
 * Enron:
 * --avroMailInput /opt/rpm1/enron/enron_mail_20110402/mail.avro --master local[4]
 *
 * JebBush (all)
 * --avroMailInput /opt/rpm1/jebbush/avro-monthly --master local[4]
 *
 * JebBush (1999)
 * --avroMailInput /opt/rpm1/jebbush/avro-monthly/1999 --master local[4]
 */
object BasicRddFunctions extends ExecutionTimer {

  val LOGGER = Logger.getLogger(AttachmentStats.getClass)

  def main(args: Array[String]): Unit = {
    startTimer()
    val appName = "BasicRddFunctions"
    val additionalSparkProps = Map[String, String]()
    val analyticInput = MailRecordAnalytic.getAnalyticInput(appName, args, additionalSparkProps, LOGGER)

    //compiler can infer bodiesRdd type - explicitly listed for example clarity
    val bodiesRdd: RDD[String] = analyticInput.mailRecordRdd.map { record =>
      record.getBody
    }
    val bodyLinesRdd: RDD[String] = bodiesRdd.flatMap { body => body.split("\n") }
    val bodyWordsRdd: RDD[String] = bodyLinesRdd.flatMap { line => line.split("""\W+""") }

    val stopWords = List("in", "it", "let", "no", "or", "the")
    val wordsRdd = bodyWordsRdd.filter(!stopWords.contains(_))

    println(s"There were ${wordsRdd.count()} words.")

    println(s"There were ${wordsRdd.distinct().count()} distinct words.")

    stopTimer()
    val prefixMsg = s"Executed over ${analyticInput.config.avroMailInput} in: "
    logTotalTime(prefixMsg, LOGGER)
  }

}
