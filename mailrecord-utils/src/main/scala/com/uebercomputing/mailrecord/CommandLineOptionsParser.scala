package com.uebercomputing.mailrecord

import java.io.File
import org.apache.hadoop.conf.Configuration

case class Config(avroMailInput: String = "/opt/rpm1/enron/enron_mail_20110402/mail.avro",
                  masterOpt: Option[String] = None,
                  hadoopConfPathOpt: Option[String] = None) {}

trait CommandLineOptionsParser {

  def getConfigOpt(args: Array[String]): Option[Config] = {
    val p = parser()
    // parser.parse returns Option[C]
    p.parse(args, Config())
  }

  def parser(): scopt.OptionParser[Config] = {
    new scopt.OptionParser[Config]("scopt") {

      head("CommandLineOptionsParser", "1.0")

      opt[String]("avroMailInput") optional () action { (avroMailInputArg, config) =>
        config.copy(avroMailInput = avroMailInputArg)
      } validate { x =>
        val f = new File(x)
        if (f.exists() && f.canRead()) success
        else failure("Option --avroMailInput must be readable directory or file")
      } text ("avroMailInput is a string with relative/absolute location of avro mail files. Either an avro file or a directory.")

      opt[String]("master") optional () action { (masterArg, config) =>
        config.copy(masterOpt = Some(masterArg))
      } text ("master indicates value for Spark's -master " +
        "argument (e.g. local, local[4] or local[*] for 1, 4 or all local processors). Only if not run via spark-submit/shell (those scripts pick up --master).")

      opt[String]("hadoopConfPath") optional () action { (hadoopConfPathArg, config) =>
        config.copy(hadoopConfPathOpt = Some(hadoopConfPathArg))
      } text ("hadoopConfPath is String with relative or absolute location of Hadoop configuration file to specify file system to use (contains fs.defaultFS). Default is file:///")

    }
  }

  def getLocalHadoopConf(): Configuration = {
    val conf = new Configuration
    conf.set("fs.defaultFS", "file:///")
    conf.set("mapreduce.framework.name", "local")
    conf
  }
}
