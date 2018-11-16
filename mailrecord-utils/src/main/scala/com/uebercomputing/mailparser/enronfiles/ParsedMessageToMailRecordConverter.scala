package com.uebercomputing.mailparser.enronfiles

import com.uebercomputing.mailrecord.MailRecord
import java.util.UUID
import org.apache.log4j.Logger

/**
 * Takes file system metadata (user, folder name, file name)
 * and map of mail message header/fields with their
 * values and converts it to a MailRecord object.
 */
object ParsedMessageToMailRecordConverter {

  val DefaultFrom = "xunknown@unknown.com"
  val DefaultSubject = "XUnkownSubject"
  val DefaultDate = 0L

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

    val subject = map.get(MessageParser.Subject).getOrElse()

    val dateStrOpt = map.get(MessageParser.Date)
    val dateUtcEpoch = dateStrOpt match {
      case Some(dateStr) => {
        val dateTry = MessageUtils.parseDateAsUtcEpochTry(dateStr)
        dateTry match {
           case Success(date) => date
           case Failure(ex) => {
             val errMsg = s"Invalid date $dateStr in $fileSystemMeta - using default epoch"
             logger.warn(errMsg)
             DefaultDate
           }
        }
      }
      case None => DefaultDate
    }

    val bodyOpt = map.get(MessageParser.Body)

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

    MailRecord(uuid, from, tosOpt, ccsOpt, bccsOpt, dateUtcEpoch, subjectOpt)
  }

}
