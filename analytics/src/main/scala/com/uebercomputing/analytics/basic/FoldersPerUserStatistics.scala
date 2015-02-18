package com.uebercomputing.analytics.basic

import org.apache.log4j.Logger
import org.apache.spark.SparkContext._
import com.uebercomputing.mailrecord.ExecutionTimer
import com.uebercomputing.mailrecord.Implicits.mailRecordToMailRecordOps
import com.uebercomputing.mailrecord.MailRecordAnalytic
import com.uebercomputing.mailparser.enronfiles.AvroMessageProcessor
import java.nio.charset.StandardCharsets

/**
 * Run with two args:
 *
 * Enron:
 * --avroMailInput /opt/rpm1/enron/filemail.avro --master local[4]
 */
object FoldersPerUserStatistics extends ExecutionTimer {

  val LOGGER = Logger.getLogger(FoldersPerUserStatistics.getClass)

  def main(args: Array[String]): Unit = {
    startTimer()
    val appName = "FoldersPerUserStatistics"
    val additionalSparkProps = Map[String, String]()
    val analyticInput = MailRecordAnalytic.getAnalyticInput(appName, args, additionalSparkProps, LOGGER)
    val userFolderTuplesRdd = analyticInput.mailRecordsRdd.flatMap { mailRecord =>
      val userNameOpt = mailRecord.getMailFieldOpt(AvroMessageProcessor.UserName)
      val folderNameOpt = mailRecord.getMailFieldOpt(AvroMessageProcessor.FolderName)
      if (userNameOpt.isDefined && folderNameOpt.isDefined) {
        Some((userNameOpt.get, folderNameOpt.get))
      } else {
        None
      }
    }
    userFolderTuplesRdd.cache()

    //mutable set - reduce object creation/garbage collection
    val uniqueFoldersByUserRdd = userFolderTuplesRdd.aggregateByKey(scala.collection.mutable.Set[String]())(
      seqOp = (folderSet, folder) => folderSet + folder,
      combOp = (set1, set2) => set1 ++ set2)
    val folderPerUserRddExact = uniqueFoldersByUserRdd.mapValues { set => set.size }.sortByKey()
    folderPerUserRddExact.saveAsTextFile("exact")

    val folderPerUserRddEstimate = userFolderTuplesRdd.countApproxDistinctByKey().sortByKey()
    folderPerUserRddEstimate.saveAsTextFile("estimate")

    analyticInput.sc.stop()
    stopTimer()
    val prefixMsg = s"Executed over ${analyticInput.config.avroMailInput} in: "
    logTotalTime(prefixMsg, LOGGER)
  }
}
