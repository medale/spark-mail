package com.uebercomputing.pst

import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import org.apache.hadoop.conf.Configuration
import org.joda.time.DateTime
import com.pff.PSTFile
import com.uebercomputing.io.FileExtensionFilter
import com.uebercomputing.io.FileUtils
import com.uebercomputing.time.DateUtils
import com.uebercomputing.io.IoConstants
import com.uebercomputing.hadoop.HadoopUtils

/**
 * Invoke command line from spark-mail:
 * java -classpath pst-utils/target/pst-utils-*-shaded.jar com.uebercomputing.pst.Main --pstDir /opt/rpm1/jebbush --avroOutDir /opt/rpm1/jebbush/avro-monthly --rollup monthly > msg.txt 2>&1
 *
 * --pstDir /opt/rpm1/jebbush
 * --avroOutDir /opt/rpm1/jebbush/avro
 * --rollup monthly
 *
 * Create small set of test avro files:
 * --pstDir src/test/resources/psts/enron1.pst
 * --avroOutDir (don't specify - uses /tmp/avro)
 * --overwrite true
 * --rollup none
 */
object Main {

  case class Config(pstDir: String = ".",
                    avroOutDir: String = IoConstants.TempDir + "/avro",
                    rollup: DatePartitionType = PartitionByMonth,
                    hadoopConfFileOpt: Option[String] = None,
                    overwrite: Boolean = false)

  val PstFilter = new FileExtensionFilter(".pst", ".PST")

  def main(args: Array[String]): Unit = {
    val startTime = new DateTime()
    val p = parser()
    // parser.parse returns Option[C]
    p.parse(args, Config()) map { config =>
      val pstFiles = getPstFiles(new File(config.pstDir))
      println(s"Processing: ${pstFiles.mkString(",")}")
      val hadoopConf = config.hadoopConfFileOpt match {
        case Some(configLocation) => HadoopUtils.getHadoopConfiguration(configLocation)
        case None                 => HadoopUtils.getLocalHadoopConfiguration()
      }
      val rootPath = config.avroOutDir
      val datePartitionType = config.rollup
      println(s"Using ${datePartitionType.getClass} rollup...")
      println(s"Input dir is ${config.pstDir}")
      println(s"Output dir is ${config.avroOutDir}")
      println(s"""File system is ${hadoopConf.get("fs.defaultFS")}""")
      for (pstFileLoc <- pstFiles) {
        val pstStartTime = new DateTime()
        val pstAbsolutePath = pstFileLoc.getAbsolutePath
        val pstFile = new PSTFile(pstFileLoc)
        val mailRecordByDateWriter = new MailRecordByDateWriter(hadoopConf, datePartitionType, rootPath, pstAbsolutePath)
        PstFileToAvroProcessor.processPstFile(mailRecordByDateWriter, pstFile, pstAbsolutePath)
        mailRecordByDateWriter.closeAllWriters()
        val pstEndTime = new DateTime()
        println(s"Time to process ${pstAbsolutePath} was ${DateUtils.getTimeDifferenceToSeconds(pstStartTime, pstEndTime)}")
        println(s"Total time since start was ${DateUtils.getTimeDifferenceToSeconds(startTime, pstEndTime)}")
      }
    }
    val endTime = new DateTime()
    println(s"Total runtime: ${DateUtils.getTimeDifferenceToSeconds(startTime, endTime)}")
  }

  def getPstFiles(pstDir: File): List[File] = {
    FileUtils.getMatchingFilesRecursively(pstDir, PstFilter)
  }

  def parser(): scopt.OptionParser[Config] = {
    new scopt.OptionParser[Config]("scopt") {

      head("scopt", "3.x")

      opt[String]("pstDir") optional () action { (pstDirArg, config) =>
        config.copy(pstDir = pstDirArg)
      } validate { x =>
        val path = Paths.get(x)
        if (Files.exists(path) && Files.isReadable(path) && Files.isDirectory(path)) success
        else failure("Option --pstDir must be readable directory")
      } text ("pstDir is String with relative or absolute location of mail dir.")

      //      opt[String]("pstPaths") optional () action { (pstPathsArg, config) =>
      //        config.copy(pstPaths = pstPathsArg.split(",").toList)
      //      } text ("pstPaths is an optional argument as comma-separated list of pst files to process.")

      opt[String]("avroOutDir") optional () action { (avroOutDirArg, config) =>
        config.copy(avroOutDir = avroOutDirArg)
      } text ("avroOutDir is String with relative or absolute location of root directory for avro output files.")

      opt[String]("rollup") optional () action { (rollupArg, config) =>
        {
          val rollupType = rollupArg match {
            case "daily"   => PartitionByDay
            case "monthly" => PartitionByMonth
            case "yearly"  => PartitionByYear
            case "none"    => NoDatePartition
          }
          config.copy(rollup = rollupType)
        }
      } validate { rollupArg =>
        if (List("daily", "monthly", "yearly", "none").contains(rollupArg)) success
        else failure("Option --rollup must be either daily, monthly or yearly (default: monthly)")
      } text ("rollup can be daily, monthly or yearly (default: monthly) and determines how emails are binned into Avro files by the email's date.")

      opt[String]("hadoopConfFile") optional () action { (hadoopConfFileArg, config) =>
        config.copy(hadoopConfFileOpt = Some(hadoopConfFileArg))
      } text ("hadoopConfFile is String with relative or absolute location of Hadoop configuration file to specify file system to use (contains fs.defaultFS). Default is file:///")

      opt[Boolean]("overwrite") optional () action { (overwriteArg, config) =>
        config.copy(overwrite = overwriteArg)
      } text ("explicit --overwrite true is needed to overwrite existing avro file.")

      checkConfig { config =>
        if (!config.overwrite) {
          if (new File(config.avroOutDir).exists()) {
            failure("avroOutDir must not exist! Use explicit --overwrite true option to overwrite existing directory.")
          } else {
            success
          }
        } else {
          success
        }
      }
    }
  }
}
