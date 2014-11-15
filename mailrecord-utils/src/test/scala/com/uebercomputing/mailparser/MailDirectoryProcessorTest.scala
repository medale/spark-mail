package com.uebercomputing.mailparser

import com.uebercomputing.mailrecord.MailRecord
import com.uebercomputing.test.UnitTest
import java.io.File
import scala.io.Source

class MailDirectoryProcessorTest extends UnitTest {

  val mailDirPath = "src/test/resources/enron/maildir"

  test("All 12 mail messages under mail directory should get processed") {

    val dirProc = new MailDirectoryProcessor(
      Nil, mailDirPath) with InMemoryMailMessageProcessor
    val messagesProcessed = dirProc.processMailDirectory()
    assert(messagesProcessed === 12)
  }

  test("bad mail directory should result in parse exception") {
    val invalidMailDir = "src/test/scala"
    val dirProc = new MailDirectoryProcessor(
      Nil, invalidMailDir) with InMemoryMailMessageProcessor
    intercept[ParseException] {
      dirProc.processMailDirectory()
    }
  }

  test("processUserDirectory should generate processed count of 6") {
    val userDir = new File(mailDirPath, "neal-s")
    val dirProc = new MailDirectoryProcessor(
      Nil, mailDirPath) with InMemoryMailMessageProcessor
    val actualProcessedCount = dirProc.processUserDirectory(userDir)
    assert(actualProcessedCount === 6)
  }

  test("isUserDirectoryToBeProcessed true") {
    val userNamesToProcess = List("lay-k", "mims-thurston-p")
    val dirProc = new MailDirectoryProcessor(userNamesToProcess, mailDirPath) with InMemoryMailMessageProcessor
    for (user <- userNamesToProcess) {
      val userDir = new File(mailDirPath, user)
      val result = dirProc.isUserDirectoryToBeProcessed(userDir)
      assert(result)
    }
  }

  test("isUserDirectoryToBeProcessed false") {
    val userNamesToProcess = List("lay-k", "mims-thurston-p")
    val dirProc = new MailDirectoryProcessor(userNamesToProcess, mailDirPath) with InMemoryMailMessageProcessor
    for (user <- userNamesToProcess) {
      val userDir = new File(mailDirPath, "bogus-user")
      val result = dirProc.isUserDirectoryToBeProcessed(userDir)
      assert(!result)
    }
  }
}

trait InMemoryMailMessageProcessor extends MessageProcessor {

  private val messageProcessor = new AvroMessageProcessor

  override def process(fileSystemMeta: FileSystemMetadata, mailIn: Source): MailRecord = {
    val parseMap = MessageParser(mailIn)
    messageProcessor.convertMapToMailRecord(fileSystemMeta, parseMap)
  }
}
