package com.uebercomputing.analytics.basic

import org.apache.log4j.Logger
import com.uebercomputing.mailrecord.MailRecordAnalytic
import com.uebercomputing.mailrecord.ExecutionTimer

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
object UniqueSenderCounter extends MailRecordAnalytic with ExecutionTimer {

  val LOGGER = Logger.getLogger(UniqueSenderCounter.getClass)

  def main(args: Array[String]): Unit = {
    startTimer()
    val appName = "UniqueSenderCounter"
    val additionalSparkProps = Map[String, String]()
    val analyticInput = getAnalyticInput(appName, args, additionalSparkProps, LOGGER)
    val allFroms = analyticInput.mailRecordRdd.map { mailRecord =>
      mailRecord.getFrom()
    }
    val allFromsCount = allFroms.count()
    val uniqueFromsCount = allFroms.distinct().count()
    println(s"All froms were $allFromsCount, unique froms were $uniqueFromsCount")
    stopTimer()
    val prefixMsg = s"Executed over ${analyticInput.config.avroMailInput} in: "
    logTotalTime(prefixMsg, LOGGER)
  }

}
