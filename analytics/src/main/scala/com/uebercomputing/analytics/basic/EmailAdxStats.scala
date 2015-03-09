package com.uebercomputing.analytics.basic

import org.apache.log4j.Logger
import com.uebercomputing.mailrecord.MailRecordAnalytic
import com.uebercomputing.mailrecord.ExecutionTimer
import org.apache.spark.SparkContext.numericRDDToDoubleRDDFunctions
import org.apache.spark.SparkContext._
import com.uebercomputing.mailrecord.Implicits._
import com.uebercomputing.mailrecord.AnalyticInput

/**
 * Run with two args:
 *
 * Enron:
 * --avroMailInput /opt/rpm1/enron/enron_mail_20110402/mail.avro --master local[4]
 *
 * JebBush (all)
 * --avroMailInput /opt/rpm1/jebbush/avro-monthly --master local[4]
 *
 * JebBush (1999)
 * --avroMailInput /opt/rpm1/jebbush/avro-monthly/1999 --master local[4]
 *
 * <br>
 * cd spark-mail
 * spark-shell --master local[4] --driver-memory 4G --executor-memory 4G \
 * --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
 * --conf spark.kryo.registrator=com.uebercomputing.mailrecord.MailRecordRegistrator \
 * --conf spark.kryoserializer.buffer.mb=128 \
 * --conf spark.kryoserializer.buffer.max.mb=512 \
 * --jars mailrecord-utils/target/mailrecord-utils-1.1.0-SNAPSHOT-shaded.jar
 *
 * scala> :paste
 *
 * import com.uebercomputing.mailrecord._
 * import com.uebercomputing.mailrecord.Implicits.mailRecordToMailRecordOps
 *
 * val args = Array("--avroMailInput", "/opt/rpm1/jebbush/avro-monthly")
 * val config = CommandLineOptionsParser.getConfigOpt(args).get
 * val mailRecordRdd = MailRecordAnalytic.getMailRecordRdd(sc, config)
 */
object EmailAdxStats extends ExecutionTimer {

  val LOGGER = Logger.getLogger(EmailAdxStats.getClass)

  val To = "To"
  val From = "From"
  val Cc = "Cc"
  val Bcc = "Bcc"

  def main(args: Array[String]): Unit = {
    val appName = "EmailAdxStats"
    val additionalSparkProps = Map[String, String]()
    val analyticInput = MailRecordAnalytic.getAnalyticInput(appName, args, additionalSparkProps, LOGGER)

  }

  def getEmailAdxStats(analyticInput: AnalyticInput): Unit = {
    startTimer()

    val emailAdxPairRdd = analyticInput.mailRecordsRdd.flatMap { mailRecord =>
      val to = mailRecord.getToOpt().getOrElse(Nil)
      val cc = mailRecord.getCcOpt().getOrElse(Nil)
      val bcc = mailRecord.getBccOpt().getOrElse(Nil)
      val adxLists = List((To, to), (Cc, cc), (Bcc, bcc))

      val typeEmailAdxTuples = adxLists.flatMap {
        typeAdxTuple =>
          val (adxType, adx) = typeAdxTuple
          getAdxTuples(adxType, adx)
      }
      (From, mailRecord.getFrom()) :: typeEmailAdxTuples
    }

    //show how this is cached in UI (http://localhost:4040/storage)
    emailAdxPairRdd.cache()

    println(s"Total emails in RDD: ${analyticInput.mailRecordsRdd.count()}")

    val uniqueAdxRdd = emailAdxPairRdd.values.distinct()
    uniqueAdxRdd.saveAsTextFile("uniqueAdx")

    val uniqueFroms = emailAdxPairRdd.filter { typeAdxTuple =>
      val (adxType, adx) = typeAdxTuple
      adxType == From
    }.values.distinct(4)
    uniqueFroms.saveAsTextFile("uniqueFroms")

    analyticInput.sc.stop()

    stopTimer()
    val prefixMsg = s"Executed over ${analyticInput.config.avroMailInput} in: "
    logTotalTime(prefixMsg, LOGGER)
  }

  def getAdxTuples(adxType: String, adx: List[String]): List[(String, String)] = {
    adx.map { addr =>
      (adxType, addr)
    }
  }

}
