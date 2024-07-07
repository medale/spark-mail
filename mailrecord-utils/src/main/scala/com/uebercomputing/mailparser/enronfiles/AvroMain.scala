package com.uebercomputing.mailparser.enronfiles

import com.uebercomputing.io.PathUtils
import com.uebercomputing.mailrecord.MailRecord
import java.io.File
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import scala.annotation.tailrec
import scala.util.Using

/**
 * Arguments:
 * --mailDir ${HOME}/datasets/enron/raw/maildir
 * --avroOutput ${HOME}/datasets/enron/mail.avro
 *
 * Create test avro file:
 * --mailDir src/test/resources/enron/maildir
 * --avroOutput enron-small.avro
 * --overwrite true
 *
 * Create small avro file by restricting users:
 * --mailDir /datasets/enron/raw//maildir
 * --avroOutput /datasets/enron/enron-small.avro
 * --users
 * allen-p,arnold-j,arora-h,beck-s,benson-r,blair-l,brawner-s,buy-r,campbell-l,carson-m,cash-m,dasovich-j,davis-d,dean-c,delainey-d,derrick-j,dickson-s,gay-r,geaccone-t,germany-c,griffith-j,grigsby-m,guzman-m,haedicke-m,hain-m,harris-s,hayslett-r,heard-m,hendrickson-s,hernandez-j,hodge-j,holst-k,horton-s,hyatt-k,kaminski-v,kean-s,keavey-p,keiser-k,king-j,lay-k
 * --overwrite true
 *
 * Invoke for full email set from sbt console:
 *
 * sbt > mailrecordUtils/console val mailDir = "/datasets/enron/raw/maildir" val avroOutput =
 * "/datasets/enron/mail.avro" val args = Array("--mailDir",mailDir, "--avroOutput",avroOutput)
 * com.uebercomputing.mailparser.enronfiles.AvroMain.main(args)
 */
object AvroMain {

  val MailDirArg = "--mailDir"
  val AvroOutputArg = "--avroOutput"
  val OverwriteArg = "--overwrite"

  case class Config(
      mailDir: Path = Paths.get("."),
      users: List[String] = List(),
      avroOutput: File = new File("mail.avro"),
      overwrite: Boolean = false
    )

  def main(args: Array[String]): Unit = {
    val p = parser()
    // parser.parse returns Option[C]
    p.parse(args, Config()).map { config =>
      val mailDirProcessor = new MailDirectoryProcessor(config.mailDir, config.users) with AvroMessageProcessor
      println(s"Counting total number of mail messages in ${config.mailDir.toAbsolutePath}")
      val totalMessageCount = getTotalMessageCount(config.mailDir)
      println(s"Getting ready to process $totalMessageCount mail messages")
      Using(new FileOutputStream(config.avroOutput)) { out =>
        mailDirProcessor.open(out)
        val noFilter = (m: MailRecord) => true
        val messagesProcessed = mailDirProcessor.processMailDirectory(noFilter)
        println(s"\nTotal messages processed: $messagesProcessed")
        mailDirProcessor.close()
      }
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

      head("scopt", "4.x")

      opt[String]("mailDir")
        .optional()
        .action { (mailDirArg, config) =>
          config.copy(mailDir = Paths.get(mailDirArg))
        }
        .validate { x =>
          val path = Paths.get(x)
          if (Files.exists(path) && Files.isReadable(path) && Files.isDirectory(path)) success
          else failure(s"Option --mailDir must be readable directory. Was ${x}.")
        }
        .text("mailDir is String with relative or absolute location of mail dir.")

      opt[String]("users")
        .optional()
        .action { (x, config) =>
          config.copy(users = x.split(",").toList)
        }
        .text("users is an optional argument as comma-separated list of users.")

      opt[String]("avroOutput")
        .optional()
        .action { (x, config) =>
          config.copy(avroOutput = new File(x))
        }
        .text("avroOutput is String with relative or absolute location of new avro output file.")

      opt[Boolean]("overwrite")
        .optional()
        .action { (x, config) =>
          config.copy(overwrite = x)
        }
        .text("explicit --overwrite true is needed to overwrite existing avro file.")

      checkConfig { config =>
        if (!config.overwrite) {
          if (config.avroOutput.exists()) {
            failure("avroOutput file must not exist! Use explicit --overwrite true option to overwrite existing file.")
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
