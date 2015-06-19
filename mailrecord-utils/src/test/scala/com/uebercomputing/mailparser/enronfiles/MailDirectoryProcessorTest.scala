package com.uebercomputing.mailparser.enronfiles

import java.nio.file.Paths

import scala.io.Source

import org.junit.runner.RunWith

import com.uebercomputing.mailrecord.MailRecord
import com.uebercomputing.test.UnitTest

class MailDirectoryProcessorTest extends UnitTest {

  val mailDirPath = Paths.get("src/test/resources/enron/maildir")
  val noFilter = (m: MailRecord) => true

  test("All 13 mail messages under mail directory should get processed") {

    val dirProc = new MailDirectoryProcessor(
      mailDirPath, Nil) with InMemoryMailMessageProcessor
    val messagesProcessed = dirProc.processMailDirectory(noFilter)
    assert(messagesProcessed === 13)
  }

  test("bad mail directory should result in parse exception") {
    val invalidMailDir = Paths.get("src/test/scala")
    val dirProc = new MailDirectoryProcessor(
      invalidMailDir, Nil) with InMemoryMailMessageProcessor
    intercept[ParseException] {
      dirProc.processMailDirectory(noFilter)
    }
  }

  test("processUserDirectory should generate processed count of 6") {
    val userDir = mailDirPath.resolve("neal-s")
    val dirProc = new MailDirectoryProcessor(
      mailDirPath, Nil) with InMemoryMailMessageProcessor
    val actualProcessedCount = dirProc.processUserDirectory(userDir, 0, noFilter)
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

  override def process(fileSystemMeta: FileSystemMetadata, mailIn: Source, filter: MailRecord => Boolean): MailRecord = {
    val parseMap = MessageParser(mailIn)
    ParsedMessageToMailRecordConverter.convert(fileSystemMeta, parseMap)
  }
}
