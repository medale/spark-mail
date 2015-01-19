package com.uebercomputing.pst

import scala.collection.mutable
import com.uebercomputing.mailrecord.MailRecordWriter
import com.uebercomputing.mailrecord.MailRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

class MailRecordByDateWriter(hadoopConf: Configuration, datePartitionType: DatePartitionType, rootPath: String, pstFilePath: String) {

  private val directoryToMailRecordWriterMap = mutable.Map[String, MailRecordWriter]()
  val fileSystem = FileSystem.get(hadoopConf)
  val pstFileName = {
    if (pstFilePath.endsWith("/")) throw new RuntimeException(s"PST file path must not end with / ${pstFilePath}")
    val lastForwardSlash = pstFilePath.lastIndexOf("/")
    if (lastForwardSlash != -1) {
      pstFilePath.substring(lastForwardSlash + 1)
    } else {
      pstFilePath
    }
  }

  def writeMailRecord(mailRecord: MailRecord): Boolean = {
    val utcOffsetInMillis = mailRecord.getDateUtcEpoch
    val datePartitions = DatePartitioner.getDatePartion(datePartitionType, utcOffsetInMillis)
    val datePartitionsKey = datePartitions.mkString("/")
    val recordWriter = getMailRecordWriter(datePartitionsKey)
    recordWriter.append(mailRecord)
    true
  }

  def getMailRecordWriter(datePartitionsKey: String): MailRecordWriter = {
    if (directoryToMailRecordWriterMap.contains(datePartitionsKey)) {
      val recordWriter = directoryToMailRecordWriterMap(datePartitionsKey)
      recordWriter
    } else {
      createAndCacheMailRecordWriter(datePartitionsKey)
    }
  }

  def createAndCacheMailRecordWriter(datePartitionsKey: String): MailRecordWriter = {
    val outputPath = new Path(rootPath + "/" + datePartitionsKey)
    if (fileSystem.exists(outputPath)) {
      throw new RuntimeException(s"Path already exists ${outputPath.toString()}")
    }
    val outputStream = fileSystem.create(outputPath)
    val recordWriter = new MailRecordWriter()
    recordWriter.open(outputStream)
    directoryToMailRecordWriterMap(datePartitionsKey) = recordWriter
    recordWriter
  }

}
