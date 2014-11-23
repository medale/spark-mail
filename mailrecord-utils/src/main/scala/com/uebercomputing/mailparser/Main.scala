package com.uebercomputing.mailparser

import java.io.File
import java.io.FileOutputStream

import resource._

import scala.annotation.tailrec

/**
 * Invoke:
 * --mailDir /opt/rpm1/enron/enron_mail_20110402/maildir
 * --avroOutput /opt/rpm1/enron/enron_mail_20110402/mail.avro
 *
 * Create test avro file:
 * --mailDir src/test/resources/enron/maildir
 * --avroOutput enron-small.avro
 * --overwrite true
 */
object Main {

  val MailDirArg = "--mailDir"
  val AvroOutputArg = "--avroOutput"
  val OverwriteArg = "--overwrite"

  case class Config(mailDir: File = new File("."),
                    users: List[String] = List(),
                    avroOutput: File = new File("mail.avro"),
                    overwrite: Boolean = false)

  def main(args: Array[String]): Unit = {
    val p = parser()
    // parser.parse returns Option[C]
    p.parse(args, Config()) map { config =>
      val mailDirProcessor = new MailDirectoryProcessor(config.mailDir, config.users) with AvroMessageProcessor
      println(s"Counting total number of mail messages in ${config.mailDir.getAbsolutePath}")
      val totalMessageCount = getTotalMessageCount(config.mailDir)
      println(s"Getting ready to process $totalMessageCount mail messages")
      for (out <- managed(new FileOutputStream(config.avroOutput))) {
        mailDirProcessor.open(out)
        val messagesProcessed = mailDirProcessor.processMailDirectory()
        println(s"\nTotal messages processed: $messagesProcessed")
        mailDirProcessor.close()
      }
    }
  }

  def getTotalMessageCount(mailDir: File): Int = {

    @tailrec def countHelper(files: List[File], count: Int): Int = {
      files match {
        case Nil => count
        case x :: xs => {
          if (x.isFile() && x.canRead()) countHelper(xs, count + 1)
          else if (x.isDirectory() && x.canRead()) {
            val newFiles = x.listFiles().toList
            countHelper(xs ++ newFiles, count)
          } else {
            countHelper(xs, count)
          }
        }
      }
    }
    val filesArray = mailDir.listFiles()
    countHelper(filesArray.toList, 0)
  }

  def parser(): scopt.OptionParser[Config] = {
    new scopt.OptionParser[Config]("scopt") {

      head("scopt", "3.x")

      opt[String]("mailDir") optional () action { (mailDirArg, config) =>
        config.copy(mailDir = new File(mailDirArg))
      } validate { x =>
        val f = new File(x)
        if (f.exists() && f.canRead() && f.isDirectory()) success
        else failure("Option --mailDir must be readable directory")
      } text ("mailDir is String with relative or absolute location of mail dir.")

      opt[String]("users") optional () action { (x, config) =>
        config.copy(users = x.split(",").toList)
      } text ("users is an optional argument as comma-separated list of users.")

      opt[String]("avroOutput") optional () action { (x, config) =>
        config.copy(avroOutput = new File(x))
      } text ("avroOutput is String with relative or absolute location of new avro output file.")

      opt[Boolean]("overwrite") optional () action { (x, config) =>
        config.copy(overwrite = x)
      } text ("explicit --overwrite true is needed to overwrite existing avro file.")

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
