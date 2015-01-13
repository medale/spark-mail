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

import com.uebercomputing.io.PathUtils

import resource.managed

@RunWith(classOf[JUnitRunner])
class BinaryMailRecordRegistratorTest extends fixture.FunSuite {

  val layInbox = "src/test/resources/enron/maildir/lay-k/inbox"

  case class FixtureParam(outPath: Path)

  def withFixture(test: OneArgTest) = {

    val mailFolder = Paths.get(layInbox)

    val prefix = "temp"
    val suffix = ".avro"
    val outPath = Files.createTempFile(prefix, suffix)

    try {
      writeBinaryMailRecords(mailFolder, outPath)
      val fixture = FixtureParam(outPath)
      //calls each test method with fixture
      withFixture(test.toNoArgTest(fixture))
    } finally {
      Files.deleteIfExists(outPath)
    }
  }

  //TODO: complete
  test("Show that Spark default Java serialization fails") { fixture =>
    println(fixture.outPath)
    val sparkConf = new SparkConf().setAppName("Buffers").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)

    val job = Job.getInstance()
    AvroJob.setInputKeySchema(job, BinaryMailRecord.getClassSchema)

    val recordsKeyValues = sc.newAPIHadoopFile(fixture.outPath.toString(),
      classOf[AvroKeyInputFormat[BinaryMailRecord]],
      classOf[AvroKey[BinaryMailRecord]],
      classOf[NullWritable],
      job.getConfiguration)

    val allMessages = recordsKeyValues.map {
      recordKeyValueTuple =>
        val mailRecord = recordKeyValueTuple._1.datum()
        mailRecord.getMessage
    }
    val msgStrings = allMessages.map { buffer =>
      val b = new Array[Byte](buffer.remaining())
      buffer.get(b)
      new String(b, java.nio.charset.StandardCharsets.UTF_8)
    }
    println("Yodel")
    val strings = msgStrings.collect()
    println(strings.mkString)
    println(msgStrings.count())
    assert(true === true)
  }

  def writeBinaryMailRecords(mailFolder: Path, outPath: Path): Unit = {
    val recordBuilder = BinaryMailRecord.newBuilder()
    for (mailRecordWriter <- managed(getBinaryMailRecordWriter(outPath))) {
      val mailPaths = PathUtils.listChildPaths(mailFolder)
      for (mailPath <- mailPaths) {
        val byteArray = Files.readAllBytes(mailPath)
        val msgBuffer = ByteBuffer.wrap(byteArray)
        recordBuilder.setMessage(msgBuffer)
        mailRecordWriter.append(recordBuilder.build())
        println(s"Wrote $mailPath")
      }
    }
  }

  def getBinaryMailRecordWriter(outPath: Path): DataFileWriter[BinaryMailRecord] = {
    val out = Files.newOutputStream(outPath)
    val datumWriter = new SpecificDatumWriter[BinaryMailRecord](classOf[BinaryMailRecord])
    val mailRecordWriter = new DataFileWriter[BinaryMailRecord](datumWriter)
    mailRecordWriter.setCodec(CodecFactory.snappyCodec())
    mailRecordWriter.create(BinaryMailRecord.getClassSchema(), out)
  }
}
