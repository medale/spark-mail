package com.uebercomputing.mailparser.enronfiles

import com.uebercomputing.mailrecord.MailRecord
import com.uebercomputing.mailrecord.MailRecordOps
import java.util.UUID
import org.apache.log4j.Logger
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success

/**
 * Takes file system metadata (user, folder name, file name) and map of mail message header/fields with their values and
 * converts it to a MailRecord object.
 */
object ParsedMessageToMailRecordConverter {

  val DefaultFrom = "XUnknownFrom@unknown.com"
  val DefaultSubject = "XUnknownSubject"
  val DefaultDate = 0L
  val DefaultBody = "XUnknownBody"

  private val logger = Logger.getLogger(ParsedMessageToMailRecordConverter.getClass)

  def convert(fileSystemMeta: FileSystemMetadata, parseMap: Map[String, String]): MailRecord = {
    // MailRecord(var uuid: String, var from: String, var to: Option[Seq[String]] = None,
    // var cc: Option[Seq[String]] = None, var bcc: Option[Seq[String]] = None,
    // var dateUtcEpoch: Long, var subject: String,
    // var mailFields: Option[Map[String, String]] = None,
    // var body: String, var attachments: Option[Seq[Attachment]] = None

    val uuid = UUID.randomUUID().toString

    val from = parseMap.getOrElse(MessageParser.From, DefaultFrom)

    val tosOpt = parseReceiverLineIfPresent(parseMap, MessageParser.To)
    val ccsOpt = parseReceiverLineIfPresent(parseMap, MessageParser.Cc)
    val bccsOpt = parseReceiverLineIfPresent(parseMap, MessageParser.Bcc)

    val subject = parseMap.getOrElse(MessageParser.Subject, DefaultSubject)

    val dateStrOpt = parseMap.get(MessageParser.Date)
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

    val body = parseMap.getOrElse(MessageParser.Body, DefaultBody)

    // add remaining fields that were parsed but don't
    // have an explicit field in the mail record
    var mailFields = Map[String, String]()
    for ((key, value) <- parseMap) {
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
    val mailRecord = MailRecordOps(uuid, from, tosOpt, ccsOpt, bccsOpt, dateUtcEpoch, subject, Some(mailFields), body)
    mailRecord
  }

  /**
   * @param parseMap
   * @param receiverType
   *   MessageParser.To/Cc/Bcc
   * @return
   */
  def parseReceiverLineIfPresent(parseMap: Map[String, String], receiverType: String): Option[Seq[String]] = {
    val receiverTypeCommaSeparatedOpt = parseMap.get(receiverType)
    val receiversOpt = receiverTypeCommaSeparatedOpt.map { receiverTypeCommaSeparated =>
      val receivers = MessageUtils.parseCommaSeparated(receiverTypeCommaSeparated)
      receivers
    }
    receiversOpt
  }

}
