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

  private val logger = Logger.getLogger(ParsedMessageToMailRecordConverter.getClass)

  private val mailRecordBuilder = MailRecord.newBuilder()

  mailRecordBuilder.setMailFields(new java.util.HashMap[String, String])

  def convert(fileSystemMeta: FileSystemMetadata, map: Map[String, String]): MailRecord = {
    val uuid = UUID.randomUUID()
    mailRecordBuilder.setUuid(uuid.toString())

    val fromOpt = map.get(MessageParser.From)
    for (from <- fromOpt) {
      mailRecordBuilder.setFrom(from)
    }
    val toCommaSeparatedOpt = map.get(MessageParser.To)
    for (toCommaSeparated <- toCommaSeparatedOpt) {
      val tos = MessageUtils.parseCommaSeparated(toCommaSeparated)
      mailRecordBuilder.setTo(tos)
    }
    val ccCommaSeparatedOpt = map.get(MessageParser.Cc)
    for (ccCommaSeparated <- ccCommaSeparatedOpt) {
      val ccs = MessageUtils.parseCommaSeparated(ccCommaSeparated)
      mailRecordBuilder.setCc(ccs)
    }
    val bccCommaSeparatedOpt = map.get(MessageParser.Bcc)
    for (bccCommaSeparated <- bccCommaSeparatedOpt) {
      val bccs = MessageUtils.parseCommaSeparated(bccCommaSeparated)
      mailRecordBuilder.setBcc(bccs)
    }

    val subjectOpt = map.get(MessageParser.Subject)
    for (subject <- subjectOpt) {
      mailRecordBuilder.setSubject(subject)
    }
    val dateStrOpt = map.get(MessageParser.Date)
    for (dateStr <- dateStrOpt) {
      try {
        val date = MessageUtils.parseDateAsUtcEpoch(dateStr)
        mailRecordBuilder.setDateUtcEpoch(date)
      } catch {
        case e: ParseException => {
          val errMsg = s"Invalid date $dateStr in $fileSystemMeta - using default epoch"
          logger.warn(errMsg)
          val date = 0L
          mailRecordBuilder.setDateUtcEpoch(date)
        }
      }
    }
    val bodyOpt = map.get(MessageParser.Body)
    for (body <- bodyOpt) {
      mailRecordBuilder.setBody(body)
    }

    //add remaining fields that were parsed but don't
    //have an explicit field in the mail record
    val mailFields = mailRecordBuilder.getMailFields
    mailFields.clear()
    for ((key, value) <- map) {
      if (!MessageProcessor.MailRecordFields.contains(key)) {
        mailFields.put(key, value)
      }
    }

    mailFields.put(MessageProcessor.UserName, fileSystemMeta.userName)
    mailFields.put(MessageProcessor.FolderName, fileSystemMeta.folderName)
    mailFields.put(MessageProcessor.FileName, fileSystemMeta.fileName)

    mailRecordBuilder.build()
  }

}
