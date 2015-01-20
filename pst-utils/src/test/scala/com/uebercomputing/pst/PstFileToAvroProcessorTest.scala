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
    val datePartitionType = DatePartitionType.PartitionByDay
    val mailRecordByDateWriter = new MailRecordByDateWriter(hadoopConf, datePartitionType, rootPath, TestFilePath)
    PstFileToAvroProcessor.processPstFile(mailRecordByDateWriter, pstFile, TestFilePath)
    mailRecordByDateWriter.closeAllWriters()
  }

  test("Odd date") {
    val email = getEmail(2110756)
    println(email.getMessageDeliveryTime)
  }

  def getTempDirStr(): String = {
    val systemTemp = System.getProperty("java.io.tmpdir")
    println(systemTemp)
    val dirPath = Paths.get(systemTemp)
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
