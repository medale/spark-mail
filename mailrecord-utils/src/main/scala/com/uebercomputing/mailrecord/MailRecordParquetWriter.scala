package com.uebercomputing.mailrecord

import java.io.Closeable
import java.io.OutputStream
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.hadoop.fs.Path
import org.apache.commons.io.IOUtils

class MailRecordParquetWriter extends Closeable {

  private var writer: AvroParquetWriter[MailRecord] = _

  def open(path: Path): Unit = {
    writer = new AvroParquetWriter[MailRecord](path, MailRecord.getClassSchema)
  }

  def append(record: MailRecord): Unit = {
    writer.write(record)
  }

  override def close() {
    IOUtils.closeQuietly(writer)
  }
}
