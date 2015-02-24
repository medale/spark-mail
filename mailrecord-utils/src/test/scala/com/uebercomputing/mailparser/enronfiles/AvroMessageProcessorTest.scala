package com.uebercomputing.mailparser.enronfiles

import scala.io.Source
import com.uebercomputing.test.AvroFileFixtureTest
import resource.managed

class AvroMessageProcessorTest extends AvroFileFixtureTest {

  val TestFileUrl = "/enron/maildir/neal-s/all_documents/99.txt"

  test("sunny day conversion from neal-s 99.") { testInfo =>
    val processor = new MessageProcessor with AvroMessageProcessor
    processor.open(testInfo.out)
    for (testFileIn <- managed(getClass().getResourceAsStream(TestFileUrl))) {
      val msgSrc = Source.fromInputStream(testFileIn)
      val fileSystemMetadata = getFileSystemMetadata(TestFileUrl)
      val mailRecord = processor.process(fileSystemMetadata, msgSrc)

      val mailFields = mailRecord.getMailFields()
      val actualFilename = mailFields.get(AvroMessageProcessor.FileName)
      assert(fileSystemMetadata.fileName === actualFilename.toString())

      val actualFolderName = mailFields.get(AvroMessageProcessor.FolderName)
      assert(fileSystemMetadata.folderName === actualFolderName.toString())

      val actualUserName = mailFields.get(AvroMessageProcessor.UserName)
      assert(fileSystemMetadata.userName === actualUserName.toString())

      assert("<19546475.1075853053633.JavaMail.evans@thyme>" === mailFields.get(MessageParser.MsgId).toString())
      assert("chris.sebesta@enron.com" === mailRecord.getFrom().toString())
      assert("RE: Alliant Energy - IES Utilities dispute re: Poi 2870 - Cherokee #1 TBS - July 99 thru April 2001"
        === mailRecord.getSubject().toString())

    }
    processor.close()

    val fileSys = testInfo.fileSystem
    assert(fileSys.exists(testInfo.hadoopPath))
    val fileStatus = fileSys.getFileStatus(testInfo.hadoopPath)
    val len = fileStatus.getLen()
    assert(len > 0)
  }

  def getFileSystemMetadata(path: String): FileSystemMetadata = {
    val pathParts = path.split("/")
    val userName = pathParts(2)
    val folderName = pathParts(3)
    val fileName = pathParts(4)
    FileSystemMetadata(userName, folderName,
      fileName)
  }
}
