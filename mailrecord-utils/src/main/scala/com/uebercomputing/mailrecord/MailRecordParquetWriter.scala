package com.uebercomputing.mailrecord

import java.io.Closeable
import java.io.OutputStream
import org.apache.avro.specific.SpecificDatumWriter
import parquet.avro.AvroParquetWriter
import org.apache.hadoop.fs.Path
import org.apache.commons.io.IOUtils
import parquet.hadoop.metadata.CompressionCodecName
import parquet.hadoop.ParquetWriter

class MailRecordParquetWriter extends Closeable {

  private var writer: AvroParquetWriter[MailRecord] = _

  def open(path: Path): Unit = {
    writer = new AvroParquetWriter[MailRecord](path, MailRecord.getClassSchema,
      CompressionCodecName.SNAPPY, ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE)
  }

  def append(record: MailRecord): Unit = {
    writer.write(record)
  }

  override def close() {
    IOUtils.closeQuietly(writer)
  }
}
