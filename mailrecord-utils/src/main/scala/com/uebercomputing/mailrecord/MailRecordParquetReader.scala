package com.uebercomputing.mailrecord

import java.io.Closeable
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetReader
import org.apache.commons.io.IOUtils
import org.apache.parquet.avro.AvroReadSupport

class MailRecordParquetReader extends Closeable {

  private var reader: ParquetReader[MailRecord] = _
  private var mailRecord: MailRecord = _

  def open(path: Path): Unit = {
    val builder = ParquetReader.builder(new AvroReadSupport[MailRecord](), path)
    reader = builder.build()
  }

  /**
   * Must call readNext to read next mail record. If it exists (true),
   * the record can be obtained by then calling next(). If false,
   * next will return null.
   */
  def readNext(): Boolean = {
    mailRecord = reader.read()
    mailRecord != null
  }

  /**
   * Must call readNext to advance to next record.
   * Check for true/false. If false, mailRecord will be null.
   */
  def next(): MailRecord = {
    mailRecord
  }

  override def close(): Unit = {
    IOUtils.closeQuietly(reader)
  }
}
