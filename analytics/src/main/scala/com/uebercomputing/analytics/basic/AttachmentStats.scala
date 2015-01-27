package com.uebercomputing.analytics.basic

import org.apache.log4j.Logger
import org.apache.spark.SparkContext.numericRDDToDoubleRDDFunctions
import com.uebercomputing.mailrecord.Implicits.mailRecordToMailRecordOps
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
object AttachmentStats extends MailRecordAnalytic with ExecutionTimer {

  val LOGGER = Logger.getLogger(UniqueSenderCounter.getClass)

  def main(args: Array[String]): Unit = {
    startTimer()
    val appName = "UniqueSenderCounter"
    val additionalSparkProps = Map[String, String]()
    val analyticInput = getAnalyticInput(appName, args, additionalSparkProps, LOGGER)
    val attachmentCountsRdd = analyticInput.mailRecordRdd.map { mailRecord =>
      val attachmentsOpt = mailRecord.getAttachmentsOpt()
      attachmentsOpt match {
        case Some(attachments) => attachments.size
        case None              => 0
      }
    }
    val stats = attachmentCountsRdd.stats()
    println(s"Attachment stats: ${stats}")
    stopTimer()
    val prefixMsg = s"Executed over ${analyticInput.config.avroMailInput} in: "
    logTotalTime(prefixMsg, LOGGER)
  }

}
