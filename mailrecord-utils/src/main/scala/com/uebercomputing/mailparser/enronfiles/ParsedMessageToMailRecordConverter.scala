package com.uebercomputing.mailparser.enronfiles

import com.uebercomputing.mailrecord.MailRecord
import java.util.UUID

import com.uebercomputing.mailrecord.MailRecordOps
import org.apache.log4j.Logger

import scala.collection.JavaConverters._
import scala.util.Failure
import scala.util.Success

/**
 * Takes file system metadata (user, folder name, file name)
 * and map of mail message header/fields with their
 * values and converts it to a MailRecord object.
 */
object ParsedMessageToMailRecordConverter {

  val DefaultFrom = "XUnknownFrom@unknown.com"
  val DefaultSubject = "XUnknownSubject"
  val DefaultDate = 0L
  val DefaultBody = "XUnknownBody"

  private val logger = Logger.getLogger(ParsedMessageToMailRecordConverter.getClass)

  def convert(fileSystemMeta: FileSystemMetadata, map: Map[String, String]): MailRecord = {
    //MailRecord(var uuid: String, var from: String, var to: Option[Seq[String]] = None,
    // var cc: Option[Seq[String]] = None, var bcc: Option[Seq[String]] = None,
    // var dateUtcEpoch: Long, var subject: String,
    // var mailFields: Option[Map[String, String]] = None,
    // var body: String, var attachments: Option[Seq[Attachment]] = None

    val uuid = UUID.randomUUID().toString

    val from = map.get(MessageParser.From).getOrElse(DefaultFrom)

    val toCommaSeparatedOpt = map.get(MessageParser.To)
    val tosOpt = toCommaSeparatedOpt.map {toCommaSeparated =>
      val tos = MessageUtils.parseCommaSeparated(toCommaSeparated)
      tos
    }
    val ccCommaSeparatedOpt = map.get(MessageParser.Cc)
    val ccsOpt = ccCommaSeparatedOpt.map {ccCommaSeparated =>
      val ccs = MessageUtils.parseCommaSeparated(ccCommaSeparated)
      ccs
    }
    val bccCommaSeparatedOpt = map.get(MessageParser.Cc)
    val bccsOpt = bccCommaSeparatedOpt.map {bccCommaSeparated =>
      val bccs = MessageUtils.parseCommaSeparated(bccCommaSeparated)
      bccs
    }

    val subject = map.get(MessageParser.Subject).getOrElse(DefaultSubject)

    val dateStrOpt = map.get(MessageParser.Date)
    val dateUtcEpoch = dateStrOpt match {
      case Some(dateStr) => {
        val dateTry = MessageUtils.parseDateAsUtcEpochTry(dateStr)
        dateTry match {
           case Success(date) => date
           case Failure(ex) => {
             val errMsg = s"Invalid date $dateStr in $fileSystemMeta - using default epoch (due to ${ex})"
             logger.warn(errMsg)
             DefaultDate
           }
        }
      }
      case None => DefaultDate
    }

    val body = map.get(MessageParser.Body).getOrElse(DefaultBody)

    //add remaining fields that were parsed but don't
    //have an explicit field in the mail record
    var mailFields = Map[String,String]()
    for ((key, value) <- map) {
      if (!MessageProcessor.MailRecordFields.contains(key)) {
        mailFields = mailFields + (key -> value)
      }
    }

    mailFields = mailFields + (MessageProcessor.UserName -> fileSystemMeta.userName)
    mailFields = mailFields + (MessageProcessor.FolderName -> fileSystemMeta.folderName)
    mailFields = mailFields + (MessageProcessor.FileName -> fileSystemMeta.fileName)

    val builder = MailRecord.newBuilder()
    builder.setUuid(uuid)
    builder.setFrom(from)
    tosOpt.foreach { tos =>
      builder.setTo(tos.asJava)
    }
    val mailRecord = MailRecordOps(uuid, from, tosOpt, ccsOpt, bccsOpt,
      dateUtcEpoch, subject, Some(mailFields), body)
    mailRecord
  }

}
