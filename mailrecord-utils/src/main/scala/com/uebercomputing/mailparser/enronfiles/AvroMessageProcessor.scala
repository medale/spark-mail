package com.uebercomputing.mailparser.enronfiles

import com.uebercomputing.mailrecord.MailRecord
import com.uebercomputing.mailrecord.MailRecordAvroWriter
import java.io.OutputStream
import org.apache.commons.io.IOUtils
import org.apache.log4j.Logger
import scala.io.Source

/**
 * A Message processor trait to save output in Avro MailRecord format. Mixed in with MailDirectoryProcessor (see
 * AvroMain).
 */
trait AvroMessageProcessor extends MessageProcessor {

  private val logger = Logger.getLogger(this.getClass())

  private var recordWriter: MailRecordAvroWriter = _
  var recordsAppendedCount = 0

  def open(out: OutputStream): Unit = {
    recordWriter = new MailRecordAvroWriter()
    recordWriter.open(out)
  }

  /**
   * Parses mailIn and, if filter is true, stores result as a mail record to the output stream provided by calling the
   * open method.
   *
   * @return
   *   MailRecord as it was written to output stream
   */
  override def process(
      fileSystemMeta: FileSystemMetadata,
      mailIn: Source,
      filter: MailRecord => Boolean
    ): MailRecord = {
    val parseMap = MessageParser(mailIn)
    val mailRecord = ParsedMessageToMailRecordConverter.convert(fileSystemMeta, parseMap)
    recordWriter.append(mailRecord)
    recordsAppendedCount += 1
    mailRecord
  }

  def close(): Unit = {
    IOUtils.closeQuietly(recordWriter)
  }

}
