package com.uebercomputing.mailrecord

import org.scalatest.fixture
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import java.nio.file.Path
import com.uebercomputing.io.PathUtils
import resource.managed
import scala.io.Source
import com.uebercomputing.io.Utf8Codec
import com.uebercomputing.mailparser.MessageParser
import java.nio.file.Files
import java.nio.ByteBuffer
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.avro.file.DataFileWriter
import org.apache.avro.file.CodecFactory
import java.nio.file.Paths
import java.nio.file.StandardOpenOption

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
