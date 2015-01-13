package com.uebercomputing.pst

import com.pff.PSTFile
import com.uebercomputing.test.UnitTest
import scala.collection.JavaConverters.asScalaBufferConverter
import org.scalatest.BeforeAndAfter
import com.pff.PSTFolder
import com.pff.PSTMessage

class PstEmailToMapProcessorTest extends UnitTest with BeforeAndAfter {

  val TestFilePath = "src/test/resources/psts/enron1.pst"
  val PstFile = new PSTFile(TestFilePath)
  val RootFolder = PstFile.getRootFolder
  var InboxFolder: PSTFolder = null

  /**
   * Personal folders
   * |  |  |- Top of Personal Folders 0
   * |  |  |  |- Deleted Items 0
   * |  |  |  |- rapp-b 0
   * |  |  |  |  |- Rapp, Bill (Non-Privileged) 0
   * |  |  |  |  |  |- Rapp, Bill 0
   * |  |  |  |  |  |  |- hr info 3
   * |  |  |  |  |  |  |- Sent Items 10
   * |  |  |  |  |  |  |- Inbox 56
   */
  before {
    val rootfolders = RootFolder.getSubFolders.asScala
    val topOfPersonalFolder = rootfolders(0)
    val personalFolders = topOfPersonalFolder.getSubFolders.asScala
    val rappBFolder = personalFolders(1)
    val nonPrivFolder = rappBFolder.getSubFolders.asScala(0)
    val rappBillFolder = nonPrivFolder.getSubFolders.asScala(0)
    InboxFolder = rappBillFolder.getSubFolders.asScala(2)
  }

  test("addTos") {
    val pstObj = InboxFolder.getNextChild
    pstObj match {
      case email: PSTMessage => {
        val processor = new PstEmailToMapProcessor()
        val emailMap = processor.process(email)
        assert(emailMap.size > 0)
      }
      case default => println(s"Found something of type ${default.getClass}")
    }
  }
}
