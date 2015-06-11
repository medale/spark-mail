package com.uebercomputing.mailrecord

import com.uebercomputing.io.IoConstants
import java.nio.file.Files
import java.nio.file.Paths
import com.uebercomputing.io.FileExtensionFilter
import com.uebercomputing.io.FileUtils
import java.io.File
import resource.managed
import java.io.FileOutputStream
import java.io.FileInputStream

/**
 * java -cp mailrecord-utils/target/mailrecord-utils-*-shaded.jar com.uebercomputing.mailrecord.MailRecordsCoalescer --avroRootDir /tmp/avro --combinedOutputFile /tmp/enron-all.avro
 */
object MailRecordsCoalescer {

  case class Config(avroRootDir: String = ".",
                    combinedOutputFile: String = IoConstants.TempDir + "/combined.avro")

  val AvroFileFilter = new FileExtensionFilter(".avro", ".AVRO")

  def main(args: Array[String]): Unit = {
    val p = parser()
    // parser.parse returns Option[Config]
    p.parse(args, Config()) map { config =>
      val avroFiles = FileUtils.getMatchingFilesRecursively(new File(config.avroRootDir), AvroFileFilter)
      println(s"Processing ${avroFiles.size} files from ${config.avroRootDir}...")
      val mailRecordWriter = new MailRecordAvroWriter
      for (out <- managed(new FileOutputStream(config.combinedOutputFile))) {
        mailRecordWriter.open(out)
        for (avroFile <- avroFiles) {
          println(s"\nProcessing ${avroFile.getAbsolutePath}...")
          appendFile(mailRecordWriter, avroFile)
        }
        mailRecordWriter.close()
      }
    }
  }

  def appendFile(mailRecordWriter: MailRecordAvroWriter, avroFile: File): Unit = {
    for (in <- managed(new FileInputStream(avroFile))) {
      val mailRecordReader = new MailRecordAvroReader
      mailRecordReader.open(in)
      var count = 0
      while (mailRecordReader.hasNext()) {
        if (count % 10 == 0) print(".")
        count += 1
        val mailRecord = mailRecordReader.next()
        mailRecordWriter.append(mailRecord)
      }
      mailRecordReader.close()
    }
  }

  def parser(): scopt.OptionParser[Config] = {
    new scopt.OptionParser[Config]("coalescer") {
      head("scopt", "3.x")
      opt[String]("avroRootDir") optional () action { (avroRootDirArg, config) =>
        config.copy(avroRootDir = avroRootDirArg)
      } validate { x =>
        val path = Paths.get(x)
        if (Files.exists(path) && Files.isReadable(path) && Files.isDirectory(path)) success
        else failure("Option --avroRootDir must be readable directory")
      } text ("avroRootDir is String with relative or absolute location of dir containing avro mail record files to be coalesced.")
      opt[String]("combinedOutputFile") optional () action { (combinedOutputFileArg, config) =>
        config.copy(combinedOutputFile = combinedOutputFileArg)
      } text ("combinedOutputFile is String with relative or absolute location of output file for coalesced avro mail records.")
    }
  }
}
