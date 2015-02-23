package com.uebercomputing.analytics.basic

import org.apache.log4j.Logger
import org.apache.spark.SparkContext._
import com.uebercomputing.mailrecord.ExecutionTimer
import com.uebercomputing.mailrecord.Implicits.mailRecordToMailRecordOps
import com.uebercomputing.mailrecord.MailRecordAnalytic
import java.nio.charset.StandardCharsets

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
 * --avroMailInput /opt/rpm1/jebbush/avro-yearly/1999 --master local[4]
 */
object PiiFinder extends ExecutionTimer {

  val LOGGER = Logger.getLogger(PiiFinder.getClass)

  val SsnRegex = """\d\d\d-\d\d-\d\d\d\d""".r

  def main(args: Array[String]): Unit = {
    startTimer()
    val appName = "PiiFinder"
    val additionalSparkProps = Map[String, String]()
    val analyticInput = MailRecordAnalytic.getAnalyticInput(appName, args, additionalSparkProps, LOGGER)
    val uuidBodyPairRdd = analyticInput.mailRecordsRdd.map { mailRecord =>
      val uuid = mailRecord.getUuid()
      val body = mailRecord.getBody()
      (uuid, body)
    }.coalesce(20)
    val piiRdd = uuidBodyPairRdd.filter { tuple =>
      val (uuid, body) = tuple
      containsPii(body)
    }
    piiRdd.saveAsSequenceFile("temp")

    analyticInput.sc.stop()

    stopTimer()
    val prefixMsg = s"Executed over ${analyticInput.config.avroMailInput} in: "
    logTotalTime(prefixMsg, LOGGER)
  }

  def containsPii(body: String): Boolean = {
    (SsnRegex findFirstIn body).nonEmpty
  }

}
