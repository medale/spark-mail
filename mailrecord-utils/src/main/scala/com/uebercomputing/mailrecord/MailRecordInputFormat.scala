package com.uebercomputing.mailrecord

import org.apache.avro.Schema
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroRecordReaderBase
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.log4j.Logger
import org.apache.hadoop.io.NullWritable
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.avro.mapreduce.AvroKeyRecordReader
import org.apache.hadoop.conf.Configuration

object MailRecordInputFormat {
  val MailRecordReaderSchema = MailRecord.getClassSchema()
}

class MailRecordInputFormat extends FileInputFormat[AvroKey[MailRecord], FileSplit] {

  val LOGGER = Logger.getLogger(MailRecordInputFormat.getClass)

  import MailRecordInputFormat._

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[AvroKey[MailRecord], FileSplit] = {
    split match {
      case fileSplit: FileSplit => new MailRecordRecordReader(MailRecordReaderSchema, fileSplit)
      case other => {
        val errMsg = s"Unable to process non-file split of type ${split.getClass}"
        LOGGER.error(errMsg)
        throw new RuntimeException(errMsg)
      }
    }
  }
}

class MailRecordRecordReader(val readerSchema: Schema, val fileSplit: FileSplit)
  extends AvroRecordReaderBase[AvroKey[MailRecord], FileSplit, MailRecord](readerSchema) {

  /** A reusable object to hold mail records of the Avro container file. */
  private val currentRecord = new AvroKey[MailRecord](null)

  /** {@inheritDoc} */
  override def nextKeyValue(): Boolean = {
    val hasNext = super.nextKeyValue()
    currentRecord.datum(getCurrentRecord())
    hasNext
  }

  /** {@inheritDoc} */
  override def getCurrentKey(): AvroKey[MailRecord] = {
    currentRecord
  }

  /** {@inheritDoc} */
  override def getCurrentValue(): FileSplit = {
    fileSplit
  }
}
