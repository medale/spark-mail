package com.uebercomputing.pst

import com.uebercomputing.test.UnitTest
import java.nio.file.Paths
import java.nio.file.Files
import org.apache.hadoop.conf.Configuration

class PstFileToAvroProcessorTest extends UnitTest with EmailProvider {

  test("processPstFile") {
    val rootPath = getTempDirStr()
    val hadoopConf = getLocalHadoopConf()
    val pstFile = getPstFile()
    val datePartitionType = PartitionByDay
    val mailRecordByDateWriter = new MailRecordByDateWriter(hadoopConf, datePartitionType, rootPath, TestFilePath)
    PstFileToAvroProcessor.processPstFile(mailRecordByDateWriter, pstFile, TestFilePath)
    mailRecordByDateWriter.closeAllWriters()
  }

  def getTempDirStr(): String = {
    val dirPath = Paths.get(PstConstants.TempDir)
    val prefix = "completePst"
    val tempDir = Files.createTempDirectory(dirPath, prefix)
    tempDir.toString()
  }

  def getLocalHadoopConf(): Configuration = {
    val conf = new Configuration
    conf.set("fs.defaultFS", "file:///")
    conf.set("mapreduce.framework.name", "local")
    conf
  }
}
