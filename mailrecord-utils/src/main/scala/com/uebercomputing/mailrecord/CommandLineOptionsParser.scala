package com.uebercomputing.analytics.util

import java.io.File

case class Config(avroMailFile: String = "/opt/rpm1/enron/enron_mail_20110402/mail.avro",
                  master: String = "local[4]")

trait MailMasterOptionParser {

  def config(args: Array[String]): Option[Config] = {
    val p = parser()
    // parser.parse returns Option[C]
    p.parse(args, Config())
  }

  def parser(): scopt.OptionParser[Config] = {
    new scopt.OptionParser[Config]("scopt") {

      head("scopt", "3.x")

      opt[String]("avroMailFile") optional () action { (avroMailFileArg, config) =>
        config.copy(avroMailFile = avroMailFileArg)
      } validate { x =>
        val f = new File(x)
        if (f.exists() && f.canRead() && f.isFile()) success
        else failure("Option --avroMailFile must be readable file")
      } text ("avroMailFile is a string with relative/absolute location of avro mail file.")

      opt[String]("master") optional () action { (masterArg, config) =>
        config.copy(master = masterArg)
      } text ("master indicates value for Spark's -master " +
        "argument (e.g. local, local[4] or local[*] for 1, 4 or all local processors)")
    }
  }
}
