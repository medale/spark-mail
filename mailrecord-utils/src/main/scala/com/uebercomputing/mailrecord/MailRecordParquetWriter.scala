package com.uebercomputing.mailrecord

import java.io.Closeable
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.hadoop.fs.Path
import org.apache.commons.io.IOUtils
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.ParquetWriter

class MailRecordParquetWriter extends Closeable {

  private var writer: ParquetWriter[MailRecord] = _

  def open(path: Path): Unit = {
    writer = AvroParquetWriter.builder(path)
      .withSchema(MailRecord.SCHEMA$)
      .withCompressionCodec(CompressionCodecName.SNAPPY).build()
  }

  def append(record: MailRecord): Unit = {
    writer.write(record)
  }

  override def close() {
    IOUtils.closeQuietly(writer)
  }
}
