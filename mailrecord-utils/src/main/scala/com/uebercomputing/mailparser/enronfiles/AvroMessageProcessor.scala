package com.uebercomputing.mailparser.enronfiles

import com.uebercomputing.mailrecord.MailRecord
import com.uebercomputing.mailrecord.MailRecordWriter

import java.io.OutputStream
import java.util.UUID

import org.apache.commons.io.IOUtils
import org.apache.logging.log4j.LogManager

import scala.io.Source

object AvroMessageProcessor {
  val UserName = "UserName"
  val FolderName = "FolderName"
  val FileName = "FileName"
  val MailRecordFields = List("Uuid", "From", "To", "Cc", "Bcc", "Date", "Subject", "Body")
}

trait AvroMessageProcessor extends MessageProcessor {

  private val Logger = LogManager.getLogger(this.getClass())

  private var recordWriter: MailRecordWriter = _
  private val mailRecordBuilder = MailRecord.newBuilder()

  mailRecordBuilder.setMailFields(new java.util.HashMap[CharSequence, CharSequence])
  var recordsAppendedCount = 0

  def open(out: OutputStream): Unit = {
    recordWriter = new MailRecordWriter()
    recordWriter.open(out)
  }

  /**
   * Parses mailIn and stores result as an Avro mail record to the output stream provided
   * by calling the open method.
   *
   * @return MailRecord as it was written to output stream (warning - this mail record will
   * be reused for the next call to process)
   */
  override def process(fileSystemMeta: FileSystemMetadata, mailIn: Source): MailRecord = {
    val parseMap = MessageParser(mailIn)
    val mailRecord = convertMapToMailRecord(fileSystemMeta, parseMap)
    val mailFields = mailRecord.getMailFields()
    recordWriter.append(mailRecord)
    recordsAppendedCount += 1
    mailRecord
  }

  def convertMapToMailRecord(fileSystemMeta: FileSystemMetadata, map: Map[String, String]): MailRecord = {
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
          Logger.warn(errMsg)
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
      if (!AvroMessageProcessor.MailRecordFields.contains(key)) {
        mailFields.put(key, value)
      }
    }

    mailFields.put(AvroMessageProcessor.UserName, fileSystemMeta.userName)
    mailFields.put(AvroMessageProcessor.FolderName, fileSystemMeta.folderName)
    mailFields.put(AvroMessageProcessor.FileName, fileSystemMeta.fileName)

    mailRecordBuilder.build()
  }

  def close() {
    IOUtils.closeQuietly(recordWriter)
  }
}
