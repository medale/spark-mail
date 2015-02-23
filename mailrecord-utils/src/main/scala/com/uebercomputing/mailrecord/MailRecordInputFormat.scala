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

class MailRecordInputFormat extends AvroKeyInputFormat[MailRecord] {

  val LOGGER = Logger.getLogger(MailRecordInputFormat.getClass)

  import MailRecordInputFormat._

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[AvroKey[MailRecord], NullWritable] = {
    new AvroKeyRecordReader[MailRecord](MailRecordReaderSchema)
  }
}
