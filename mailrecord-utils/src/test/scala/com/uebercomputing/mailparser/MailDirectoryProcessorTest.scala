package com.uebercomputing.mailparser

import com.uebercomputing.mailrecord.MailRecord
import com.uebercomputing.test.UnitTest
import java.io.File
import java.nio.file.Paths
import scala.io.Source

class MailDirectoryProcessorTest extends UnitTest {

  val mailDirPath = Paths.get("src/test/resources/enron/maildir")

  test("All 13 mail messages under mail directory should get processed") {

    val dirProc = new MailDirectoryProcessor(
      mailDirPath, Nil) with InMemoryMailMessageProcessor
    val messagesProcessed = dirProc.processMailDirectory()
    assert(messagesProcessed === 13)
  }

  test("bad mail directory should result in parse exception") {
    val invalidMailDir = Paths.get("src/test/scala")
    val dirProc = new MailDirectoryProcessor(
      invalidMailDir, Nil) with InMemoryMailMessageProcessor
    intercept[ParseException] {
      dirProc.processMailDirectory()
    }
  }

  test("processUserDirectory should generate processed count of 6") {
    val userDir = mailDirPath.resolve("neal-s")
    val dirProc = new MailDirectoryProcessor(
      mailDirPath, Nil) with InMemoryMailMessageProcessor
    val actualProcessedCount = dirProc.processUserDirectory(userDir, 0)
    assert(actualProcessedCount === 6)
  }

  test("isUserDirectoryToBeProcessed true") {
    val userNamesToProcess = List("lay-k", "mims-thurston-p")
    val dirProc = new MailDirectoryProcessor(mailDirPath, userNamesToProcess) with InMemoryMailMessageProcessor
    for (user <- userNamesToProcess) {
      val userDir = mailDirPath.resolve(user)
      val result = dirProc.isUserDirectoryToBeProcessed(userDir)
      assert(result)
    }
  }

  test("isUserDirectoryToBeProcessed false") {
    val userNamesToProcess = List("lay-k", "mims-thurston-p")
    val dirProc = new MailDirectoryProcessor(mailDirPath, userNamesToProcess) with InMemoryMailMessageProcessor
    for (user <- userNamesToProcess) {
      val userDir = mailDirPath.resolve("bogus-user")
      val result = dirProc.isUserDirectoryToBeProcessed(userDir)
      assert(!result)
    }
  }
}

trait InMemoryMailMessageProcessor extends MessageProcessor {

  private val messageProcessor = new MessageProcessor with AvroMessageProcessor

  override def process(fileSystemMeta: FileSystemMetadata, mailIn: Source): MailRecord = {
    val parseMap = MessageParser(mailIn)
    messageProcessor.convertMapToMailRecord(fileSystemMeta, parseMap)
  }
}
