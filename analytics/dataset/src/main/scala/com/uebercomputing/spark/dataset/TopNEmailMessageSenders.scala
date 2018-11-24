package com.uebercomputing.spark.dataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
  * An analytic that determines the top N email senders (using "from" field).
  */
object TopNEmailMessageSenders {

  /**
    *
    * @param spark
    * @param parquetFiles
    * @param n
    * @return
    */
  def findTopNEmailMessageSendersTry(spark: SparkSession,
                                     parquetFiles: Seq[String],
                                     n: Int): Try[Seq[(String, Long)]] = {

    try {
      if (!parquetFiles.isEmpty && n >= 1) {
        //Note ': _*' syntax to register Seq as vararg String*
        val records = spark.read.parquet(parquetFiles: _*)

        val topNSendersAsRows = records.select("from").
          groupBy("from").
          count.
          orderBy(desc("count")).
          limit(n).collect()

        val topNSenders = topNSendersAsRows.map { row => (row.getString(0), row.getLong(1)) }
        Success(topNSenders)
      } else {
        val errMsg = "parquetFiles sequence must not be empty and n must be >= 1"
        val ex = new IllegalArgumentException(errMsg)
        Failure(ex)
      }
    } catch {
      case t: Throwable => Failure(t)
    }
  }
}
