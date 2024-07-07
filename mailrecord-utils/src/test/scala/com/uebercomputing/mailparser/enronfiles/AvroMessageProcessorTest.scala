package com.uebercomputing.mailparser.enronfiles

import com.uebercomputing.mailrecord.MailRecord
import com.uebercomputing.test.AvroFileFixtureTest
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.Using

class AvroMessageProcessorTest extends AvroFileFixtureTest {

  val TestFileUrl = "/enron/maildir/neal-s/all_documents/99.txt"

  test("sunny day conversion from neal-s 99.") { testInfo =>
    val processor = new MessageProcessor with AvroMessageProcessor
    processor.open(testInfo.out)
    Using(getClass.getResourceAsStream(TestFileUrl)) { testFileIn =>
      val msgSrc = Source.fromInputStream(testFileIn)
      val fileSystemMetadata = getFileSystemMetadata(TestFileUrl)
      val noFilter = (m: MailRecord) => true
      val mailRecord = processor.process(fileSystemMetadata, msgSrc, noFilter)

      val mailFieldsOpt = Option(mailRecord.getMailFields)
      mailFieldsOpt match {
        case Some(mailFieldsJava) => {
          val mailFields = mailFieldsJava.asScala

          val fieldNames = Seq(MessageProcessor.FileName, MessageProcessor.FolderName, MessageProcessor.UserName)
          val expectedValues =
            Seq(fileSystemMetadata.fileName, fileSystemMetadata.folderName, fileSystemMetadata.userName)

          val keyExpectedValues = fieldNames.zip(expectedValues)
          keyExpectedValues.foreach { case (key, expectedValue) =>
            val actualValueOpt = mailFields.get(key)
            actualValueOpt match {
              case Some(actualValue) => {
                assert(actualValue === expectedValue)
              }
              case None => fail(s"mailFields did not contain ${MessageProcessor.FileName} key")
            }
          }

          assert(mailFields.isDefinedAt(MessageParser.MsgId))
          assert("<19546475.1075853053633.JavaMail.evans@thyme>" === mailFields(MessageParser.MsgId))

          assert("chris.sebesta@enron.com" === mailRecord.getFrom)
          assert(
            "RE: Alliant Energy - IES Utilities dispute re: Poi 2870 - Cherokee #1 TBS - July 99 thru April 2001"
              === mailRecord.getSubject
          )
        }
        case _ => fail("Mail record did not have mailFields")
      }

    }
    processor.close()

    val fileSys = testInfo.fileSystem
    assert(fileSys.exists(testInfo.hadoopPath))
    val fileStatus = fileSys.getFileStatus(testInfo.hadoopPath)
    val len = fileStatus.getLen
    assert(len > 0)
  }

  def getFileSystemMetadata(path: String): FileSystemMetadata = {
    val pathParts = path.split("/")
    val userName = pathParts(2)
    val folderName = pathParts(3)
    val fileName = pathParts(4)
    FileSystemMetadata(userName, folderName, fileName)
  }

}
