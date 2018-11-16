package com.uebercomputing.analytics.basic

import com.uebercomputing.mailrecord.Implicits._
import com.uebercomputing.mailrecord.{MailRecordAnalytic, MailRecordOps}
import org.apache.log4j.Logger

/**
 * Boss detector. Finds the users who have the lowest ratio of (cc) / (received).
 * The feeling being that bosses are CC'd way more than they directly receive email.
 *
 * Required args:
 *
 * --avroMailInput AVRO_FILE --master local[*]
 */
object BossDetector {
  val LOGGER = Logger.getLogger(BossDetector.getClass)

  // how many should we see
  val n = 10

  def main(args: Array[String]) {
    val appName = "BossDetector"
    val additionalSparkProps = Map[String, String]()
    val analyticInput = MailRecordAnalytic.getAnalyticInput(appName, args, additionalSparkProps, LOGGER)

    val addressTuples = analyticInput.mailRecordsRdd.flatMap { mailRecord =>
      val mro: MailRecordOps = mailRecord
      val toList = mro.getToOpt().getOrElse(Nil)
      val ccList = mro.getCcOpt().getOrElse(Nil)

      (mailRecord.getFrom, "FROM") ::
        toList
        .map(addr => (addr, "TO")) ++
        ccList
        .map(addr => (addr, "CC"))
    }

    val fieldCounts = addressTuples
      .groupByKey
      .map {
        case (addr, fields) =>
          val counts = fields.groupBy(x => x).map(tuple => (tuple._1, tuple._2.size))
          (addr, counts)
      }

    val topN = fieldCounts
      .sortBy { case (addr, counts) => -BossScore(counts) }
      .take(n)

    topN
      .foreach(tuple => println(tuple))
  }
}

/**
 * Toggle this to experiment with our boss detection.
 */
object BossScore extends Serializable {

  val zero = 0.01

  /**
   * Takes a map of field counts (TO, FROM, CC) and computes the boss score.
   *
   * @return the computed score
   */
  def apply(fieldCounts: Map[String, Int]): Double = {
    val cc = fieldCounts.get("CC").map(_.toDouble).getOrElse(zero)
    val to = fieldCounts.get("TO").map(_.toDouble).getOrElse(zero)
    cc / to
  }
}

