package com.uebercomputing.mailrecord

import com.uebercomputing.io.IoConstants
import java.nio.file.Files
import java.nio.file.Paths
import com.uebercomputing.io.FileExtensionFilter
import com.uebercomputing.io.FileUtils
import java.io.File

object MailRecordsCoalescer {

  case class Config(avroRootDir: String = ".",
                    combinedOutputFile: String = IoConstants.TempDir + "/combined.avro")

  val AvroFileFilter = new FileExtensionFilter(".avro", ".AVRO")

  def main(args: Array[String]): Unit = {
    val p = parser()
    // parser.parse returns Option[C]
    p.parse(args, Config()) map { config =>
      val avroFiles = FileUtils.getMatchingFilesRecursively(new File(config.avroRootDir), AvroFileFilter)
      //TODO append all avro files to combinedOutputFile
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
