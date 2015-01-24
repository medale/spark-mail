package com.uebercomputing.mailrecord

import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileWriter
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroJob
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.junit.runner.RunWith
import org.scalatest.fixture
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._
import com.uebercomputing.io.PathUtils

import resource.managed

@RunWith(classOf[JUnitRunner])
class MailRecordRegistratorTest extends fixture.FunSuite {

  val layInbox = "src/test/resources/enron/maildir/lay-k/inbox"

  case class FixtureParam(outPath: Path)

  def withFixture(test: OneArgTest) = {

    val mailFolder = Paths.get(layInbox)

    val prefix = "temp"
    val suffix = ".avro"
    val outPath = Files.createTempFile(prefix, suffix)

    try {
      writeMailRecords(mailFolder, outPath)
      val fixture = FixtureParam(outPath)
      //calls each test method with fixture
      withFixture(test.toNoArgTest(fixture))
    } finally {
      Files.deleteIfExists(outPath)
    }
  }

  //TODO: TEST will not fail locally - no serialization!
  ignore("Show that Spark default Java serialization fails") { fixture =>
    println(fixture.outPath)
    val sparkConf = new SparkConf().setAppName("Buffers").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)

    val job = Job.getInstance()
    AvroJob.setInputKeySchema(job, MailRecord.getClassSchema)

    val recordsKeyValues = sc.newAPIHadoopFile(fixture.outPath.toString(),
      classOf[AvroKeyInputFormat[MailRecord]],
      classOf[AvroKey[MailRecord]],
      classOf[NullWritable],
      job.getConfiguration)

    val attachmentData = recordsKeyValues.flatMap {
      recordKeyValueTuple =>
        val mailRecord = recordKeyValueTuple._1.datum()
        val byteBufferList = mailRecord.getAttachments.asScala.map {
          attachment =>
            attachment.getData
        }
        byteBufferList
    }
  }

  //TODO:
  def writeMailRecords(mailFolder: Path, outPath: Path): Unit = {
    val recordBuilder = MailRecord.newBuilder()
    for (mailRecordWriter <- managed(getMailRecordWriter(outPath))) {
      val mailPaths = PathUtils.listChildPaths(mailFolder)
      for (mailPath <- mailPaths) {
        val byteArray = Files.readAllBytes(mailPath)
        val msgBuffer = ByteBuffer.wrap(byteArray)
        val attachment = new Attachment()
        val attachments = new java.util.ArrayList[Attachment]()
        attachment.setData(msgBuffer)
        attachments.add(attachment)
        recordBuilder.setAttachments(attachments)
        mailRecordWriter.append(recordBuilder.build())
        println(s"Wrote $mailPath")
      }
    }
  }

  def getMailRecordWriter(outPath: Path): DataFileWriter[MailRecord] = {
    val out = Files.newOutputStream(outPath)
    val datumWriter = new SpecificDatumWriter[MailRecord](classOf[MailRecord])
    val mailRecordWriter = new DataFileWriter[MailRecord](datumWriter)
    mailRecordWriter.setCodec(CodecFactory.snappyCodec())
    mailRecordWriter.create(MailRecord.getClassSchema(), out)
  }
}
