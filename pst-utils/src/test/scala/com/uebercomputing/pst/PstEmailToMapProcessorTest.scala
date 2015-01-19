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

class PstEmailToMapProcessorTest extends UnitTest with EmailProvider {

  import PstEmailToMapProcessor._

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
    val uuid = UUID.randomUUID().toString
    val tos = new java.util.ArrayList[String]()
    tos.add("Gregory.Steagall@ENRON.com")
    tos.add("Imad_Tareen@enron.net")
    val ccs = new java.util.ArrayList[String]()
    ccs.add("Martin.Katz@ENRON.com")
    ccs.add("Raquel.Lopez@ENRON.com")
    val expectedKeys = List(DescriptorNodeIdKey, ToKey, UuidKey, InternetMessageIdKey,
      DateKey, CcKey, OriginalPstPathKey, SubjectKey, FromKey, ParentFoldersKey, BodyKey)
    val descriptorIndex = 2099172L
    val email = getEmail(descriptorIndex)
    val emailMap = PstEmailToMapProcessor.process(uuid, email, TestFilePath, ParentFolders)
    assert(emailMap(DescriptorNodeIdKey) === "2099172")
    assert(emailMap(ToKey) === tos)
    assert(emailMap(UuidKey) === uuid)
    println(s":::${emailMap(InternetMessageIdKey)}:::")
    assert(emailMap(InternetMessageIdKey) === "<DDQPUCIORF2UC1I1BQVJ0AAVMWCKER0JA@zlsvr22>")
    assert(emailMap(DateKey) != null)
    assert(emailMap(CcKey) === ccs)
    assert(emailMap(OriginalPstPathKey) === "src/test/resources/psts/enron1.pst")
    assert(emailMap(SubjectKey) === "Allen Center Availability")
    assert(emailMap(FromKey) === "mbx_parktrans@ENRON.com")
    println(emailMap(ParentFoldersKey).getClass)
    assert(emailMap(ParentFoldersKey) === ParentFolders)
    assert(emailMap(BodyKey).asInstanceOf[String].startsWith("Good Morning,"))

    expectedKeys.foreach { key => emailMap.remove(key) }
    assert(emailMap.size === 0)
  }

  /**
   * "PSA AdditionalProvisions 091701.DOC", "size": 254464, "mimeType": "application\/octet-stream"
   * Key: DescriptorNodeId -> 2097828
   * Key: Attachments -> 1
   */
  test("Email 2097828 with an attachment") {
    val msgId = UUID.randomUUID().toString
    val descriptorIndex = 2097828L
    val email = getEmail(descriptorIndex)
    val emailMap = PstEmailToMapProcessor.process(msgId, email, TestFilePath, ParentFolders)
    assert(emailMap.contains(AttachmentsKey))
    val attachments = emailMap(AttachmentsKey).asInstanceOf[java.util.List[Attachment]]
    assert(attachments.size() === 1)
    val attachment = attachments.get(0)
    assert(attachment.getFileName === "PSA AdditionalProvisions 091701.DOC")
    assert(attachment.getSize === 254464)
    assert(attachment.getMimeType === """application/octet-stream""")
    assert(attachment.getData.remaining === 254464)
  }

  test("PrintAll") {
    val inboxFolder = getInboxFolder()
    val count = inboxFolder.getContentCount
    for (i <- 0 until count) {
      val pstObj = inboxFolder.getNextChild
      pstObj match {
        case email: PSTMessage => {
          val msgId = UUID.randomUUID().toString
          val emailMap = PstEmailToMapProcessor.process(msgId, email, TestFilePath, ParentFolders)
          emailMap.foreach(entry =>
            if (entry._1 != PstEmailToMapProcessor.BodyKey && entry._1 != PstEmailToMapProcessor.AttachmentsKey) {
              println(s"Key: ${entry._1} -> ${entry._2}")
            } else if (entry._1 == PstEmailToMapProcessor.AttachmentsKey) {
              val attachments = entry._2.asInstanceOf[java.util.List[Attachment]].asScala
              print(s"Key: ${entry._1} -> ")
              for (attachment <- attachments) {
                println(s"${attachment.getFileName}, ${attachment.getMimeType}, ${attachment.getSize}")
              }
            })
          println(">>End<<")
          assert(emailMap.size > 0)
        }
        case default => println(s"Found something of type ${default.getClass}")
      }
    }
  }
}
