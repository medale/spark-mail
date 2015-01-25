package com.uebercomputing.pst

import com.uebercomputing.mailrecord.MailRecordSparkConfFactory
import org.apache.spark.SparkContext
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import org.apache.hadoop.conf.Configuration
import org.joda.time.DateTime
import com.pff.PSTFile
import com.uebercomputing.time.DateUtils
import org.apache.spark.SerializableWritable

/**
 * Invoke command line from spark-mail:
 * java -classpath pst-utils/target/pst-utils-0.9.0-SNAPSHOT-shaded.jar com.uebercomputing.pst.SparkEtl --pstDir /opt/rpm1/jebbush --avroOutDir /opt/rpm1/jebbush/spark-avro-monthly --rollup monthly > msg-spark.txt 2>&1
 *
 * Note: this code assumes that .pst files are read from a local directory. If running in cluster that directory might
 * be a shared NFS drive.
 */
object SparkEtl {

  /**
   * Only specify master here if running without spark-submit or spark-shell
   */
  case class Config(masterOpt: Option[String] = None,
                    pstDir: String = ".",
                    avroOutDir: String = PstConstants.TempDir + "/avro",
                    rollup: DatePartitionType = PartitionByMonth,
                    hadoopConfFileOpt: Option[String] = None,
                    overwrite: Boolean = false)

  def main(args: Array[String]): Unit = {
    val startTime = new DateTime()
    val p = parser()
    // parser.parse returns Option[C]
    p.parse(args, Config()) map { config =>
      val appName = "SparkEtl"
      val keyVals = if (config.masterOpt.isDefined) {
        List(MailRecordSparkConfFactory.AppNameKey -> appName, MailRecordSparkConfFactory.MasterKey -> config.masterOpt.get)
      } else {
        List(MailRecordSparkConfFactory.AppNameKey -> appName)
      }
      val props = Map[String, String](keyVals: _*)
      val sparkConf = MailRecordSparkConfFactory(props)
      val sc = new SparkContext(sparkConf)

      val pstDir = new File(config.pstDir)
      val pstFiles = getPstFiles(pstDir)

      val hadoopConf = config.hadoopConfFileOpt match {
        case Some(confFilePath) => {
          val conf = new Configuration()
          conf.addResource(confFilePath)
          conf
        }
        case None => getLocalHadoopConf()
      }
      val serializableConf = new SerializableWritable(hadoopConf)

      val rootPath = config.avroOutDir
      val datePartitionType = config.rollup

      val filesRdd = sc.parallelize(pstFiles)
      filesRdd.foreach { pstFileLoc =>
        val pstStartTime = new DateTime()
        val pstAbsolutePath = pstFileLoc.getAbsolutePath
        val pstFile = new PSTFile(pstFileLoc)
        val mailRecordByDateWriter = new MailRecordByDateWriter(serializableConf.value, datePartitionType, rootPath, pstAbsolutePath)
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
    val pstFilter = new PstFileFilter()
    pstDir.listFiles(pstFilter).toList
  }

  def getLocalHadoopConf(): Configuration = {
    val conf = new Configuration
    conf.set("fs.defaultFS", "file:///")
    conf.set("mapreduce.framework.name", "local")
    conf
  }

  def parser(): scopt.OptionParser[Config] = {
    new scopt.OptionParser[Config]("scopt") {

      head("scopt", "3.x")

      opt[String]("master") optional () action { (masterArg, config) =>
        config.copy(masterOpt = Some(masterArg))
      } text ("master is String describing Spark master - only needed when not running via spark-submit/shell.")

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
          }
          config.copy(rollup = rollupType)
        }
      } validate { rollupArg =>
        if (List("daily", "monthly", "yearly").contains(rollupArg)) success
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
