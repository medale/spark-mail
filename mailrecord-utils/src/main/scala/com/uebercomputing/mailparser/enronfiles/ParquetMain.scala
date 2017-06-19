package com.uebercomputing.mailparser.enronfiles

import java.io.File
import java.nio.file.{Files, Path, Paths}

import com.uebercomputing.hadoop.HadoopUtils
import com.uebercomputing.io.PathUtils
import com.uebercomputing.mailrecord.MailRecord

import scala.annotation.tailrec

/**
  * Invoke:
  * java -cp mailrecord-utils/target/mailrecord-utils-1.2.1-SNAPSHOT-shaded.jar \
  * com.uebercomputing.mailparser.enronfiles.ParquetMain \
  * --mailDir /opt/rpm1/enron/enron_mail_20150507/maildir \
  * --parquetOutput /opt/rpm1/enron/enron_mail_20150507/mail.parquet
  *
  * Create test avro file:
  * --mailDir src/test/resources/enron/maildir
  * --parquetOutput enron-small.parquet
  * --overwrite true
  *
  * Create small avro file by restricting users:
  * --mailDir /opt/rpm1/enron/enron_mail_20110402/maildir
  * --parquetOutput /opt/rpm1/enron/enron-small.parquet
  * --users allen-p,arnold-j,arora-h,beck-s,benson-r,blair-l,brawner-s,buy-r,campbell-l,carson-m,cash-m,dasovich-j,davis-d,dean-c,delainey-d,derrick-j,dickson-s,gay-r,geaccone-t,germany-c,griffith-j,grigsby-m,guzman-m,haedicke-m,hain-m,harris-s,hayslett-r,heard-m,hendrickson-s,hernandez-j,hodge-j,holst-k,horton-s,hyatt-k,kaminski-v,kean-s,keavey-p,keiser-k,king-j,lay-k
  * --overwrite true
  */
object ParquetMain {

  val MailDirArg = "--mailDir"
  val ParquetOutputArg = "--parquetOutput"
  val OverwriteArg = "--overwrite"

  case class Config(mailDir: Path = Paths.get("."),
                    users: List[String] = List(),
                    parquetOutput: String = "mail.parquet",
                    hadoopConfFileOpt: Option[String] = None,
                    overwrite: Boolean = false)

  def main(args: Array[String]): Unit = {
    val p = parser()
    // parser.parse returns Option[C]
    p.parse(args, Config()) map { config =>
      val mailDirProcessor = new MailDirectoryProcessor(config.mailDir, config.users) with ParquetMessageProcessor
      println(s"Counting total number of mail messages in ${config.mailDir.toAbsolutePath}")
      val totalMessageCount = getTotalMessageCount(config.mailDir)
      println(s"Getting ready to process $totalMessageCount mail messages")
      val hadoopConf = config.hadoopConfFileOpt match {
        case Some(configLocation) => HadoopUtils.getHadoopConfiguration(configLocation)
        case None => HadoopUtils.getLocalHadoopConfiguration()
      }
      val path = HadoopUtils.getAsHadoopPath(config.parquetOutput, hadoopConf)
      mailDirProcessor.open(path)
      val noFilter = (m: MailRecord) => true
      val messagesProcessed = mailDirProcessor.processMailDirectory(noFilter)
      println(s"\nTotal messages processed: $messagesProcessed")
      mailDirProcessor.close()
    }
  }

  def getTotalMessageCount(mailDir: Path): Int = {

    @tailrec def countHelper(files: List[Path], count: Int): Int = {
      files match {
        case Nil => count
        case x :: xs => {
          if (Files.isReadable(x)) {
            if (Files.isRegularFile(x)) countHelper(xs, count + 1)
            else if (Files.isDirectory(x)) {
              val newFiles = PathUtils.listChildPaths(x)
              countHelper(xs ++ newFiles, count)
            } else {
              countHelper(xs, count)
            }
          } else {
            countHelper(xs, count)
          }
        }
      }
    }

    val files = PathUtils.listChildPaths(mailDir)
    countHelper(files, 0)
  }

  def parser(): scopt.OptionParser[Config] = {
    new scopt.OptionParser[Config]("scopt") {

      head("scopt", "3.x")

      opt[String]("mailDir") optional() action { (mailDirArg, config) =>
        config.copy(mailDir = Paths.get(mailDirArg))
      } validate { x =>
        val path = Paths.get(x)
        if (Files.exists(path) && Files.isReadable(path) && Files.isDirectory(path)) success
        else failure("Option --mailDir must be readable directory")
      } text ("mailDir is String with relative or absolute location of mail dir.")

      opt[String]("users") optional() action { (x, config) =>
        config.copy(users = x.split(",").toList)
      } text ("users is an optional argument as comma-separated list of users.")

      opt[String]("parquetOutput") optional() action { (x, config) =>
        config.copy(parquetOutput = x)
      } text ("parquetOutput is String with relative or absolute location of new parquet output file.")

      opt[String]("hadoopConfFile") optional() action { (hadoopConfFileArg, config) =>
        config.copy(hadoopConfFileOpt = Some(hadoopConfFileArg))
      } text ("hadoopConfFile is String with relative or absolute location of Hadoop configuration file to specify file system to use (contains fs.defaultFS). Default is file:///")

      opt[Boolean]("overwrite") optional() action { (x, config) =>
        config.copy(overwrite = x)
      } text ("explicit --overwrite true is needed to overwrite existing avro file.")

      checkConfig { config =>
        if (!config.overwrite) {
          // TODO - use Hadoop filesystem to check existence
          if (new File(config.parquetOutput).exists()) {
            failure("parquetOutput file must not exist! Use explicit --overwrite true option to overwrite existing file.")
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
