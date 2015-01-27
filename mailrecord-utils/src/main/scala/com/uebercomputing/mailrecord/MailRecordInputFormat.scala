package com.uebercomputing.mailrecord

import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.avro.mapred.AvroKey
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.avro.Schema
import org.apache.log4j.Logger
import org.apache.avro.mapreduce.AvroRecordReaderBase
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.Path

object MailRecordInputFormat {

  val InputKeySchemaKey = "avro.schema.input.key"

  /**
   * Called by SparkContext.newAPIHadoopFile - forwarded to underlying file input format.
   */
  def addInputPath(job: Job, path: Path): Unit = {
    FileInputFormat.addInputPath(job, path)
  }

  /**
   * By default FileInputFormat is not recursive. Must set explicitly unless we are
   * processing just a path to one avro file.
   */
  def setInputDirRecursive(job: Job, inputDirRecursive: Boolean): Unit = {
    FileInputFormat.setInputDirRecursive(job, inputDirRecursive)
  }
}

class MailRecordInputFormat extends FileInputFormat[AvroKey[MailRecord], FileSplit] {

  val LOGGER = Logger.getLogger(MailRecordInputFormat.getClass)

  import MailRecordInputFormat._

  def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[AvroKey[MailRecord], FileSplit] = {
    val hadoopConf = context.getConfiguration()
    val readerSchemaStr = hadoopConf.get(InputKeySchemaKey)
    var readerSchema = MailRecord.getClassSchema()
    if (readerSchemaStr != null) {
      val schemaParser = new Schema.Parser()
      try {
        readerSchema = schemaParser.parse(readerSchemaStr)
      } catch {
        case e: Exception => {
          val errMsg = s"Unable to parse ${InputKeySchemaKey} from hadoop configuration due to ${e}"
          LOGGER.error(errMsg)
          throw new RuntimeException(errMsg)
        }
      }
    }
    split match {
      case fileSplit: FileSplit => new MailRecordRecordReader(readerSchema, fileSplit)
      case other => {
        val errMsg = s"Unable to process non-file split of type ${split.getClass}"
        LOGGER.error(errMsg)
        throw new RuntimeException(errMsg)
      }
    }
  }
}

class MailRecordRecordReader(val readerSchema: Schema, val fileSplit: FileSplit) extends AvroRecordReaderBase[AvroKey[MailRecord], FileSplit, MailRecord](readerSchema) {

  /** A reusable object to hold records of the Avro container file. */
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
