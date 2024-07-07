package com.uebercomputing.spark.dataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import scala.collection.compat.immutable.ArraySeq
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * An analytic that determines the top N email senders (using "from" field).
 *
 * To run: <ol> <li>sbt assembly</li> <li>spark-shell --jars
 * analytics/dataset/target/scala-2.11/dataset-2.0.0-SNAPSHOT-fat.jar</li> <li>spark-submit</li> </ol>
 *
 * <pre> sbt > datasetAnalytics/console > import com.uebercomputing.spark.dataset.TopNEmailMessageSenders > val args =
 * Array("7", "/datasets/enron/enron-small.parquet") > TopNEmailMessageSenders.main(args) </pre>
 */
object TopNEmailMessageSenders {

  /**
   * @param spark
   * @param parquetFiles
   * @param n
   * @return
   */
  def findTopNEmailMessageSendersTry(
      spark: SparkSession,
      parquetFiles: Seq[String],
      n: Int
    ): Try[Seq[(String, Long)]] = {

    try {
      if (!parquetFiles.isEmpty && n >= 1) {
        // Note ': _*' syntax to register Seq as vararg String*
        val records = spark.read.parquet(parquetFiles: _*)

        val topNSendersAsRows = records.select("from").groupBy("from").count().orderBy(desc("count")).limit(n).collect()

        val topNSenders = topNSendersAsRows.map { row => (row.getString(0), row.getLong(1)) }
        Success(ArraySeq.unsafeWrapArray(topNSenders))
      } else {
        val errMsg = "parquetFiles sequence must not be empty and n must be >= 1"
        val ex = new IllegalArgumentException(errMsg)
        Failure(ex)
      }
    } catch {
      case t: Throwable => Failure(t)
    }
  }

  /**
   * Expects invocation with at least two or more args: ... 10 file0 file1 etc.
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TopNEmailMessageSenders").master("local[2]").getOrCreate()
    val n = args.head.toInt
    val parquetFiles = ArraySeq.unsafeWrapArray(args.tail)
    val topNTry = findTopNEmailMessageSendersTry(spark, parquetFiles, n)

    topNTry match {
      case Success(topN) => {
        val out = topN.map { case (from, count) => s"${from}: ${count}" }.mkString("\n")
        println(out)
      }
      case Failure(ex) => {
        println(s"Unable to find top ${n} due to ${ex}")
      }
    }
  }

}
