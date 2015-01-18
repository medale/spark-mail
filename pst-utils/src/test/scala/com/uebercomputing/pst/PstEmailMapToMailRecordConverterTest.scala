package com.uebercomputing.pst

import com.uebercomputing.test.UnitTest
import java.util.UUID
import java.util.Date

class PstEmailMapToMailRecordConverterTest extends UnitTest with EmailProvider {

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
    println("here")
    val uuid = UUID.randomUUID().toString
    val tos = new java.util.ArrayList[String]()
    tos.add("Gregory.Steagall@ENRON.com")
    tos.add("Imad_Tareen@enron.net")
    val ccs = new java.util.ArrayList[String]()
    ccs.add("Martin.Katz@ENRON.com")
    ccs.add("Raquel.Lopez@ENRON.com")

    val descriptorIndex = 2099172L
    val email = getEmail(descriptorIndex)
    val emailMap = PstEmailToMapProcessor.process(uuid, email, TestFilePath, ParentFolders)
    val mailRecord = PstEmailMapToMailRecordConverter.toMailRecord(emailMap)
    val expectedDateUtcMillis = emailMap(DateKey).asInstanceOf[Date].getTime
    assert(mailRecord.getUuid === uuid)
    assert(mailRecord.getFrom === "mbx_parktrans@ENRON.com")
    assert(mailRecord.getTo === tos)
    assert(mailRecord.getCc === ccs)
    assert(mailRecord.getBcc === null)
    assert(mailRecord.getDateUtcEpoch === expectedDateUtcMillis)
    assert(mailRecord.getSubject === "Allen Center Availability")
    assert(mailRecord.getBody.startsWith("Good Morning,"))

    val mailFieldsMap = mailRecord.getMailFields
    assert(mailFieldsMap.get(DescriptorNodeIdKey) === "2099172")
    assert(mailFieldsMap.get(InternetMessageIdKey) === "<DDQPUCIORF2UC1I1BQVJ0AAVMWCKER0JA@zlsvr22>")
    assert(mailFieldsMap.get(ParentFoldersKey) === ParentFolders)
    assert(mailFieldsMap.get(OriginalPstPathKey) === "src/test/resources/psts/enron1.pst")

    assert(mailRecord.getAttachments === null)
  }

  /**
   * "PSA AdditionalProvisions 091701.DOC", "size": 254464, "mimeType": "application\/octet-stream"
   * Key: DescriptorNodeId -> 2097828
   * Key: Attachments -> 1
   */
  test("Email 2097828 with an attachment") {
    val uuid = UUID.randomUUID().toString
    val descriptorIndex = 2097828L
    val email = getEmail(descriptorIndex)
    val emailMap = PstEmailToMapProcessor.process(uuid, email, TestFilePath, ParentFolders)
    val mailRecord = PstEmailMapToMailRecordConverter.toMailRecord(emailMap)

    val attachments = mailRecord.getAttachments
    assert(attachments.size() === 1)
    val attachment = attachments.get(0)
    assert(attachment.getFileName === "PSA AdditionalProvisions 091701.DOC")
    assert(attachment.getSize === 254464)
    assert(attachment.getMimeType === """application/octet-stream""")
    assert(attachment.getData.remaining === 254464)
  }
}
