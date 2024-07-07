package com.uebercomputing.test

import java.nio.file.{Path => NioPath}
import java.nio.file.Files
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FSDataOutputStream

case class AvroFileTestInfo(
    val localPath: NioPath,
    val fileSystem: FileSystem,
    val hadoopPath: HadoopPath,
    val out: FSDataOutputStream,
    val hadoopConf: Configuration
  )

trait AvroMailRecordsFileProvider {

  def createAndOpenTestFile(): AvroFileTestInfo = {
    val localPath = getTempFilePath
    val avroFileUri = localPath.toFile().getAbsolutePath()
    val hadoopConf = getLocalHadoopConfiguration()
    val fileSystem = getLocalFileSystem(hadoopConf)
    val hadoopPath = new HadoopPath(avroFileUri)
    val out = fileSystem.create(hadoopPath)
    AvroFileTestInfo(localPath, fileSystem, hadoopPath, out, hadoopConf)
  }

  def closeAndDeleteTestFile(fileTestInfo: AvroFileTestInfo) = {
    IOUtils.closeQuietly(fileTestInfo.out)
    Files.deleteIfExists(fileTestInfo.localPath)
  }

  def getTempFilePath(): NioPath = {
    val tempFilePath = Files.createTempFile("testContainer", ".avro")
    tempFilePath
  }

  def getLocalHadoopConfiguration(): Configuration = {
    val conf = new Configuration()
    conf.set("default.fsName", "file:///")
    conf
  }

  def getLocalFileSystem(hadoopConf: Configuration): FileSystem = {
    val fileSys = FileSystem.get(hadoopConf)
    fileSys
  }

}
