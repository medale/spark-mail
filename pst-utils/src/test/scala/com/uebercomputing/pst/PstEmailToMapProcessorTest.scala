package com.uebercomputing.pst

import com.pff.PSTFile
import com.uebercomputing.test.UnitTest
import scala.collection.JavaConverters._
import org.scalatest.BeforeAndAfter
import com.pff.PSTFolder
import com.pff.PSTMessage
import com.pff.PSTObject
import java.util.Date
import java.util.UUID
import java.util.Arrays
import com.uebercomputing.mailrecord.Attachment

class PstEmailToMapProcessorTest extends UnitTest with BeforeAndAfter {

  import PstEmailToMapProcessor._

  val TestFilePath = "src/test/resources/psts/enron1.pst"
  val PstFile = new PSTFile(TestFilePath)
  val RootFolder = PstFile.getRootFolder
  var InboxFolder: PSTFolder = null
  val parentFolders = Arrays.asList("Personal folders", "Top of Personal Folders", "Deleted Items", "rapp-b", "Rapp, Bill (Non-Privileged)", "Rapp, Bill", "Inbox")

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

  /**
   * Key: DescriptorNodeId -> 2099172
   * Key: To -> [Gregory.Steagall@ENRON.com, Imad_Tareen@enron.net]
   * Key: Message-ID -> ....
   * Key: InternetMessageId -> <DDQPUCIORF2UC1I1BQVJ0AAVMWCKER0JA@zlsvr22>
   * Key: Date -> Mon Oct 29 11:56:22 EST 2001
   * Key: Cc -> [Martin.Katz@ENRON.com, Raquel.Lopez@ENRON.com]
   * Key: OriginalPstPath -> src/test/resources/psts/enron1.pst
   * Key: Subject -> Allen Center Availability
   * Key: From -> mbx_parktrans@ENRON.com
   * Key: ParentFolders -> ["Personal folders", "Top of Personal Folders", "Deleted Items", "rapp-b", "Rapp, Bill (Non-Privileged)", "Rapp, Bill", "Inbox"]
   * Key: BodyKey -> "Good Morning, ...."
   */
  test("Email 2099172 with two To and two Cc") {
    val msgId = UUID.randomUUID().toString
    val descriptorIndex = 2099172L
    val tos = new java.util.ArrayList[String]()
    tos.add("Gregory.Steagall@ENRON.com")
    tos.add("Imad_Tareen@enron.net")
    val ccs = new java.util.ArrayList[String]()
    ccs.add("Martin.Katz@ENRON.com")
    ccs.add("Raquel.Lopez@ENRON.com")
    val pstObj = PSTObject.detectAndLoadPSTObject(PstFile, descriptorIndex)
    val expectedKeys = List(DescriptorNodeIdKey, ToKey, MsgIdKey, InternetMessageIdKey,
      DateKey, CcKey, OriginalPstPathKey, SubjectKey, FromKey, ParentFoldersKey, BodyKey)
    pstObj match {
      case email: PSTMessage => {
        val emailMap = PstEmailToMapProcessor.process(msgId, email, TestFilePath, parentFolders)
        assert(emailMap(DescriptorNodeIdKey) === "2099172")
        assert(emailMap(ToKey) === tos)
        assert(emailMap(MsgIdKey) === msgId)
        println(s":::${emailMap(InternetMessageIdKey)}:::")
        assert(emailMap(InternetMessageIdKey) === "<DDQPUCIORF2UC1I1BQVJ0AAVMWCKER0JA@zlsvr22>")
        assert(emailMap(DateKey) != null)
        assert(emailMap(CcKey) === ccs)
        assert(emailMap(OriginalPstPathKey) === "src/test/resources/psts/enron1.pst")
        assert(emailMap(SubjectKey) === "Allen Center Availability")
        assert(emailMap(FromKey) === "mbx_parktrans@ENRON.com")
        println(emailMap(ParentFoldersKey).getClass)
        assert(emailMap(ParentFoldersKey) === parentFolders)
        assert(emailMap(BodyKey).asInstanceOf[String].startsWith("Good Morning,"))

        expectedKeys.foreach { key => emailMap.remove(key) }
        assert(emailMap.size === 0)
      }
      case default => fail(s"Expected email but found type ${default.getClass}")
    }
  }

  /**
   * Key: DescriptorNodeId -> 2097828
   * Key: Attachments -> 1
   */
  test("Email 2097828 with an attachment") {
    val msgId = UUID.randomUUID().toString
    val descriptorIndex = 2097828L
    val pstObj = PSTObject.detectAndLoadPSTObject(PstFile, descriptorIndex)
    pstObj match {
      case email: PSTMessage => {
        val emailMap = PstEmailToMapProcessor.process(msgId, email, TestFilePath, parentFolders)
        assert(emailMap.contains(AttachmentsKey))
        val attachments = emailMap(AttachmentsKey).asInstanceOf[java.util.List[Attachment]]
        assert(attachments.size() === 1)
        val attachment = attachments.get(0)
        //TODO - complete attachment check
      }
      case default => fail(s"Expected email but found type ${default.getClass}")
    }
  }

  ignore("PrintAll") {
    val count = InboxFolder.getContentCount
    for (i <- 0 until count) {
      val pstObj = InboxFolder.getNextChild
      pstObj match {
        case email: PSTMessage => {
          val msgId = UUID.randomUUID().toString
          val emailMap = PstEmailToMapProcessor.process(msgId, email, TestFilePath, parentFolders)
          emailMap.foreach(entry =>
            if (entry._1 != PstEmailToMapProcessor.BodyKey && entry._1 != PstEmailToMapProcessor.AttachmentsKey) {
              println(s"Key: ${entry._1} -> ${entry._2}")
            } else if (entry._1 == PstEmailToMapProcessor.AttachmentsKey) {
              println(s"Key: ${entry._1} -> ${entry._2.asInstanceOf[java.util.List[String]].size}")
            })

          println(">>End<<")
          assert(emailMap.size > 0)
        }
        case default => println(s"Found something of type ${default.getClass}")
      }
    }
  }
}
