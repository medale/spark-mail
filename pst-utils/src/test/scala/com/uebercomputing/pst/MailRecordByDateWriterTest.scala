package com.uebercomputing.pst

import com.uebercomputing.test.UnitTest
import org.apache.hadoop.conf.Configuration
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import com.uebercomputing.mailrecord.MailRecordAvroReader
import org.apache.commons.io.IOUtils
import java.nio.file.Path
import org.joda.time.DateTime
import java.util.TimeZone
import org.joda.time.DateTimeZone
import com.uebercomputing.utils.PartitionByMonth
import com.uebercomputing.utils.PartitionByDay
import com.uebercomputing.utils.PartitionByYear

class MailRecordByDateWriterTest extends UnitTest with MailRecordProvider {

  import PstEmailToMapProcessor._

  val TestPstFile = "src/test/resources/psts/enron1.pst"

  test("getPstAvroFileName valid with .pst extension") {
    val pstFilePath = "src/test/resources/psts/enron1.pst"
    val pstAvroFileName = MailRecordByDateWriter.getPstAvroFileName(pstFilePath)
    val expected = "enron1.avro"
    assert(pstAvroFileName === expected)
  }

  test("getPstAvroFileName valid with no extension") {
    val pstFilePath = "src/test/resources/psts/enron1"
    val pstAvroFileName = MailRecordByDateWriter.getPstAvroFileName(pstFilePath)
    val expected = "enron1.avro"
    assert(pstAvroFileName === expected)
  }

  test("getPstAvroFileName valid with no parent dirs") {
    val pstFilePath = "enronFile1.pst"
    val pstAvroFileName = MailRecordByDateWriter.getPstAvroFileName(pstFilePath)
    val expected = "enronFile1.avro"
    assert(pstAvroFileName === expected)
  }

  test("getPstAvroFileName invalid") {
    val pstFilePath = "src/test/resources/psts/"
    intercept[RuntimeException] {
      MailRecordByDateWriter.getPstAvroFileName(pstFilePath)
    }
  }

  /**
   * Note: We store time in UTC, which is +5 hrs during EST!
   * +4 hrs during EDT
   *
   * 2097604, Wed Sep 26 06:04:00 EDT 2001 = 10: UTC
   * 2097636, Fri Oct 05 13:02:00 EDT 2001 = 17: UTC
   * 2099236, Mon Oct 29 13:45:59 EST 2001 = 18: UTC
   * 2099332, Mon Oct 29 18:41:30 EST 2001, attachment = 23: UTC
   */
  test("byMonthPartition") {
    val hadoopConf = getLocalHadoopConf()
    val datePartitionType = PartitionByMonth
    val rootPath = getTempDirStr
    val byDateWriter = new MailRecordByDateWriter(hadoopConf, datePartitionType, rootPath, TestPstFile)
    val descriptorIndices = List(2097604, 2097636, 2099236, 2099332)
    for (descriptorIndex <- descriptorIndices) {
      val mailRecord = getMailRecord(descriptorIndex)
      val result = byDateWriter.writeMailRecord(mailRecord)
      assert(result)
    }
    byDateWriter.closeAllWriters()
    val tempPath = Paths.get(rootPath)
    val septemberPath = tempPath.resolve("2001/09")
    val octoberPath = tempPath.resolve("2001/10")
    assert(Files.exists(septemberPath))
    assert(Files.exists(octoberPath))

    val enronAvro = "enron1.avro"

    //september path should have file enron1.avro with one entry 2097604
    val septIds = List("2097604")
    val septAvro = septemberPath.resolve(enronAvro)
    assertDescriptorNodeIds(septIds, septAvro)

    //october path should have file enron1.avro with three entries 2097636, 2099300, 2099332
    val octIds = List("2097636", "2099236", "2099332")
    val octAvro = octoberPath.resolve(enronAvro)
    assertDescriptorNodeIds(octIds, octAvro)

    val mailRecord = getMailRecord(2099300)
    val dateTime = new DateTime(mailRecord.getDateUtcEpoch, DateTimeZone.UTC)
    println(dateTime)
  }

  /**
   * Note: We store time in UTC, which is +5 hrs during EST!
   * +4 hrs during EDT
   *
   * 2097604, Wed Sep 26 06:04:00 EDT 2001 = 10: UTC
   * 2097636, Fri Oct 05 13:02:00 EDT 2001 = 17: UTC
   * 2099236, Mon Oct 29 13:45:59 EST 2001 = 18: UTC
   * 2099332, Mon Oct 29 18:41:30 EST 2001, attachment = 23: UTC
   */
  test("byDayPartition") {
    val hadoopConf = getLocalHadoopConf()
    val datePartitionType = PartitionByDay
    val rootPath = getTempDirStr
    val byDateWriter = new MailRecordByDateWriter(hadoopConf, datePartitionType, rootPath, TestPstFile)
    val descriptorIndices = List(2097604, 2097636, 2099236, 2099332)
    for (descriptorIndex <- descriptorIndices) {
      val mailRecord = getMailRecord(descriptorIndex)
      val result = byDateWriter.writeMailRecord(mailRecord)
      assert(result)
    }
    byDateWriter.closeAllWriters()
    val tempPath = Paths.get(rootPath)
    val sept26Path = tempPath.resolve("2001/09/26")
    val oct05Path = tempPath.resolve("2001/10/05")
    val oct29Path = tempPath.resolve("2001/10/29")
    assert(Files.exists(sept26Path))
    assert(Files.exists(oct05Path))
    assert(Files.exists(oct29Path))

    val enronAvro = "enron1.avro"

    //september 26 path should have file enron1.avro with one entry 2097604
    val sept26Ids = List("2097604")
    val sept26Avro = sept26Path.resolve(enronAvro)
    assertDescriptorNodeIds(sept26Ids, sept26Avro)

    //oct05 path should have file enron1.avro with one entry 2097636
    val oct05Ids = List("2097636")
    val oct05Avro = oct05Path.resolve(enronAvro)
    assertDescriptorNodeIds(oct05Ids, oct05Avro)

    //oct29 path should have file enron1.avro with two entries 2099300, 2099332
    val oct29Ids = List("2099236", "2099332")
    val oct29Avro = oct29Path.resolve(enronAvro)
    assertDescriptorNodeIds(oct29Ids, oct29Avro)
  }

  /**
   * Note: We store time in UTC, which is +5 hrs during EST!
   * +4 hrs during EDT
   *
   * 2097604, Wed Sep 26 06:04:00 EDT 2001 = 10: UTC
   * 2097636, Fri Oct 05 13:02:00 EDT 2001 = 17: UTC
   * 2099236, Mon Oct 29 13:45:59 EST 2001 = 18: UTC
   * 2099332, Mon Oct 29 18:41:30 EST 2001, attachment = 23: UTC
   */
  test("byYearPartition") {
    val hadoopConf = getLocalHadoopConf()
    val datePartitionType = PartitionByYear
    val rootPath = getTempDirStr
    val byDateWriter = new MailRecordByDateWriter(hadoopConf, datePartitionType, rootPath, TestPstFile)
    val descriptorIndices = List(2097604, 2097636, 2099236, 2099332)
    for (descriptorIndex <- descriptorIndices) {
      val mailRecord = getMailRecord(descriptorIndex)
      val result = byDateWriter.writeMailRecord(mailRecord)
      assert(result)
    }
    byDateWriter.closeAllWriters()
    val tempPath = Paths.get(rootPath)
    val path = tempPath.resolve("2001")
    assert(Files.exists(path))

    val enronAvro = "enron1.avro"

    //enron1.avro should contain all four records
    val ids = List("2097604", "2097636", "2099236", "2099332")
    val avro = path.resolve(enronAvro)
    assertDescriptorNodeIds(ids, avro)
  }

  def assertDescriptorNodeIds(ids: List[String], avroPath: Path): Unit = {
    assert(Files.exists(avroPath))
    val in = Files.newInputStream(avroPath)
    val recordReader = new MailRecordAvroReader()
    recordReader.open(in)
    for (descriptorNodeId <- ids) {
      assert(recordReader.hasNext())
      val record = recordReader.next()
      assert(record.getMailFields.get(DescriptorNodeIdKey) === descriptorNodeId)
    }
    assert(!recordReader.hasNext())
    recordReader.close()
    IOUtils.closeQuietly(in)
  }

  def getLocalHadoopConf(): Configuration = {
    val conf = new Configuration
    conf.set("fs.defaultFS", "file:///")
    conf.set("mapreduce.framework.name", "local")
    conf
  }

  def getTempDirStr(): String = {
    val systemTemp = System.getProperty("java.io.tmpdir")
    val dirPath = Paths.get(systemTemp)
    val prefix = "mailrecords"
    val tempDir = Files.createTempDirectory(dirPath, prefix)
    tempDir.toString()
  }
}
