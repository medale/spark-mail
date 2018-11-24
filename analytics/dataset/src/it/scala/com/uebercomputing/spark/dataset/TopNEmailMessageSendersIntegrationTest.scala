package com.uebercomputing.spark.dataset

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.scalatest.FunSpec
import org.scalatest.Matchers

import scala.util.Failure
import scala.util.Success

/**
  * enron-small.parquet top 10
  * +-----------------------------+-----+
  * |from                         |count|
  * +-----------------------------+-----+
  * |vince.kaminski@enron.com     |14340|
  * |jeff.dasovich@enron.com      |10888|
  * |chris.germany@enron.com      |8688 |
  * |steven.kean@enron.com        |6722 |
  * |sally.beck@enron.com         |4253 |
  * |john.arnold@enron.com        |3505 |
  * |david.delainey@enron.com     |2991 |
  * |enron.announcements@enron.com|2803 |
  * |pete.davis@enron.com         |2753 |
  * |phillip.allen@enron.com      |2145 |
  * +-----------------------------+-----+
  */
class TopNEmailMessageSendersIntegrationTest extends FunSpec with Matchers with DatasetSuiteBase {

  val EnronSmallParquet = "enron-small.parquet"

  val Top10Expected = {
    val expectedAsString = """vince.kaminski@enron.com     |14340|
                             |jeff.dasovich@enron.com      |10888|
                             |chris.germany@enron.com      |8688 |
                             |steven.kean@enron.com        |6722 |
                             |sally.beck@enron.com         |4253 |
                             |john.arnold@enron.com        |3505 |
                             |david.delainey@enron.com     |2991 |
                             |enron.announcements@enron.com|2803 |
                             |pete.davis@enron.com         |2753 |
                             |phillip.allen@enron.com      |2145 |""".stripMargin
    val lines = expectedAsString.split("\n")
    val pairs = lines.map {l => l.split("\\|").take(2)}
    val expectedTuples = pairs.map {pair =>
      val sender = pair(0).trim
      val count = pair(1).trim.toLong
      (sender, count)
    }
    expectedTuples
  }

  describe("findTopNEmailMessageSendersTry") {

    it("should return Failure with illegal argument exception for empty parquetFiles") {
      val topNTry = TopNEmailMessageSenders.findTopNEmailMessageSendersTry(spark, parquetFiles = Nil, n = 1)
      topNTry match {
        case Failure(ex) => assert(ex.isInstanceOf[IllegalArgumentException])
        case Success(_) => fail("Expected Failure with IllegalArgEx")
      }
    }

    it("should return Failure with IllegalArgEx for n < 1") {
      val topNTry = TopNEmailMessageSenders.findTopNEmailMessageSendersTry(spark,
        parquetFiles = Seq(EnronSmallParquet), n = 0)
      topNTry match {
        case Failure(ex) => assert(ex.isInstanceOf[IllegalArgumentException])
        case Success(_) => fail("Expected Failure with IllegalArgEx")
      }
    }

    it("should return 1 top sender only") {
      val topOneTry =
        TopNEmailMessageSenders.findTopNEmailMessageSendersTry(spark, parquetFiles = Seq(EnronSmallParquet), n = 1)
      topOneTry match {
        case Success(topOne) => {
          assert(topOne.size === 1)
          val topOneTuple = topOne.head
          assert(topOneTuple === Top10Expected(0))
        }
        case Failure(ex) => fail(ex)
      }
    }

    it("should return correct top 10 senders") {
      val topTenTry =
        TopNEmailMessageSenders.findTopNEmailMessageSendersTry(spark,
          parquetFiles = Seq(EnronSmallParquet), n = Top10Expected.size)
      topTenTry match {
        case Success(topTen) => {
          assert(topTen.size === Top10Expected.size)
          assert(topTen === Top10Expected)
        }
        case Failure(ex) => fail(ex)
      }
    }

    //TODO test with more than one parquet file
  }
}
