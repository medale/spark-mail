package com.uebercomputing.mailrecord

import java.io.Closeable
import java.io.InputStream

import org.apache.avro.file.DataFileStream
import org.apache.avro.specific.SpecificDatumReader
import org.apache.commons.io.IOUtils

/**
 * Used to read MailRecord objects from an Avro
 * collection file.
 */
class MailRecordAvroReader extends Closeable {

  private var mailRecordStream: DataFileStream[MailRecord] = _

  def open(in: InputStream): Unit = {
    val datumReader = new SpecificDatumReader[MailRecord](classOf[MailRecord])
    mailRecordStream = new DataFileStream[MailRecord](in, datumReader)
  }

  def hasNext(): Boolean = {
    mailRecordStream.hasNext()
  }

  def next(): MailRecord = {
    mailRecordStream.next()
  }

  override def close(): Unit = {
    IOUtils.closeQuietly(mailRecordStream)
  }
}
