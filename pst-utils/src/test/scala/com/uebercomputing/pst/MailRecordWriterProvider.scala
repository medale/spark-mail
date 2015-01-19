package com.uebercomputing.pst

import java.io.OutputStream
import com.uebercomputing.mailrecord.MailRecordWriter
import java.io.File
import java.io.FileOutputStream

trait MailRecordWriterProvider {

  def getTemporaryFile(): File = {
    File.createTempFile("mailRecords", ".avro")
  }

  def getOpenMailRecordWriter(out: OutputStream): MailRecordWriter = {
    val mailRecordWriter = new MailRecordWriter()
    mailRecordWriter.open(out)
    mailRecordWriter
  }
}
