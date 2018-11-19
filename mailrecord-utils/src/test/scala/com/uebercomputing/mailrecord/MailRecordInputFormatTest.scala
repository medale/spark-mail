package com.uebercomputing.mailrecord

import java.nio.file.Paths

import com.uebercomputing.mailparser.enronfiles.AvroMessageProcessor
import com.uebercomputing.mailparser.enronfiles.MailDirectoryProcessor
import com.uebercomputing.test.AvroFileTestInfo
import com.uebercomputing.test.AvroMailRecordsFileProvider
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.mapreduce.TaskID
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.scalatest.fixture.FunSuite
import org.scalatest.junit.JUnitRunner

class MailRecordInputFormatTest extends FunSuite with AvroMailRecordsFileProvider {

  val mailDir = Paths.get("src/test/resources/enron/maildir")

  type FixtureParam = AvroFileTestInfo

  var testInfo: AvroFileTestInfo = _

  override def withFixture(test: OneArgTest) = {
    try {
      testInfo = createAndOpenTestFile()
      writeMailDirAsMailRecords(testInfo)
      test(testInfo)
    } finally {
      closeAndDeleteTestFile(testInfo)
    }
  }

  test("reading all records in a given Avro container file") { testInfoInput =>
    val job = Job.getInstance(testInfoInput.hadoopConf)
    FileInputFormat.addInputPath(job, testInfoInput.hadoopPath)
    //not needed in this test case but in general
    //FileInputFormat.setInputDirRecursive(job, true)
    val contentSummary = testInfoInput.fileSystem.getContentSummary(testInfoInput.hadoopPath)
    val length = contentSummary.getLength
    val split = new FileSplit(testInfoInput.hadoopPath, 0, length, Array[String] { "localhost" })
    val taskId = new TaskID()
    val taskAttemptId = new TaskAttemptID(taskId, 0)
    val context = new TaskAttemptContextImpl(job.getConfiguration, taskAttemptId)
    val inputFormat = new MailRecordInputFormat()
    val reader = inputFormat.createRecordReader(split, context)
    reader.initialize(split, context)
    val froms = scala.collection.mutable.ArrayBuffer.empty[String]
    while (reader.nextKeyValue()) {
      val avroKey = reader.getCurrentKey()
      val storageRecord = avroKey.datum()
      val from = storageRecord.from
      froms += from

      val currFileSplit = reader.getCurrentValue
      assert(currFileSplit.getPath === testInfoInput.hadoopPath)
    }
    //1 for each mail message
    assert(froms.size === 13)
    val sorted = froms.sorted
    assert(sorted(0) === "adam.johnson@enron.com")
  }

  def writeMailDirAsMailRecords(testInfo: AvroFileTestInfo): Unit = {
    val users = Nil
    val mailDirProcessor = new MailDirectoryProcessor(mailDir, users) with AvroMessageProcessor
    mailDirProcessor.open(testInfo.out)
    val noFilter = (m: MailRecord) => true
    val messagesProcessed = mailDirProcessor.processMailDirectory(noFilter)
    println(s"\nTotal messages processed: $messagesProcessed")
    mailDirProcessor.close()
  }
}
