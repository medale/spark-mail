package com.uebercomputing.analytics.basic

import org.apache.log4j.Logger

import com.uebercomputing.mailrecord.ExecutionTimer
import com.uebercomputing.mailrecord.Implicits.mailRecordToMailRecordOps
import com.uebercomputing.mailrecord.MailRecordAnalytic

/**
 * Run with two args:
 *
 * Enron:
 * --avroMailInput /opt/rpm1/enron/enron_mail_20110402/mail.avro --master local[4]
 */
object AttachmentStats extends ExecutionTimer {

  val LOGGER = Logger.getLogger(AttachmentStats.getClass)

  def main(args: Array[String]): Unit = {
    startTimer()
    val appName = "AttachmentStats"
    val additionalSparkProps = Map[String, String]()
    val analyticInput = MailRecordAnalytic.getAnalyticInput(appName, args, additionalSparkProps, LOGGER)
    val attachmentCountsRdd = analyticInput.mailRecordsRdd.flatMap { mailRecord =>
      val attachmentsOpt = mailRecord.getAttachmentsOpt()
      attachmentsOpt match {
        case Some(attachments) => Some(attachments.size)
        case None              => None
      }
    }
    val stats = attachmentCountsRdd.stats()
    println(s"Attachment stats: ${stats}")
    analyticInput.sc.stop()
    stopTimer()
    val prefixMsg = s"Executed over ${analyticInput.config.avroMailInput} in: "
    logTotalTime(prefixMsg, LOGGER)
  }

}
