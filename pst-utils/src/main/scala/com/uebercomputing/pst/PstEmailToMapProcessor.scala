package com.uebercomputing.pst

import com.pff.PSTMessage
import scala.collection.mutable
import scala.collection.JavaConverters._
import java.util.UUID
import java.io.InputStream
import java.nio.ByteBuffer
import java.io.ByteArrayOutputStream
import org.apache.commons.io.IOUtils
import com.uebercomputing.mailrecord.Attachment
import org.apache.log4j.Logger
import com.pff.PSTRecipient

object PstEmailToMapProcessor {
  val MsgIdKey = "Message-ID"
  val DateKey = "Date"
  val FromKey = "From"
  val ToKey = "To"
  val CcKey = "Cc"
  val BccKey = "Bcc"
  val SubjectKey = "Subject"
  val BodyKey = "Body"
  val ParentFoldersKey = "ParentFolders"
  val InternetMessageIdKey = "InternetMessageId"
  val AttachmentsKey = "Attachments"
  val DescriptorNodeIdKey = "DescriptorNodeId"
  val OriginalPstPathKey = "OriginalPstPath"

  val LOGGER = Logger.getLogger(PstEmailToMapProcessor.getClass)

  /**
   * Can include String, List[String], Date
   */
  def process(uuid: String, email: PSTMessage, originalPstPath: String, parentFolders: java.util.List[String]): mutable.Map[String, AnyRef] = {
    val map = mutable.Map[String, AnyRef]()
    map(MsgIdKey) = uuid
    map(FromKey) = email.getSenderEmailAddress.trim()
    map(DateKey) = email.getMessageDeliveryTime //Date object
    map(SubjectKey) = email.getSubject.trim() //String
    map(BodyKey) = email.getBody //String
    map(InternetMessageIdKey) = email.getInternetMessageId.trim() //String
    map(DescriptorNodeIdKey) = "" + email.getDescriptorNodeId
    map(ParentFoldersKey) = parentFolders
    map(OriginalPstPathKey) = originalPstPath
    addRecipients(email, map)
    addAttachmentsIfPresent(email, map)
    map
  }

  def addAttachmentsIfPresent(email: PSTMessage, map: mutable.Map[String, AnyRef]): Unit = {
    if (email.hasAttachments()) {
      val attachmentList = new java.util.ArrayList[Attachment]()
      val attachmentCount = email.getNumberOfAttachments
      for (i <- 0 until attachmentCount) {
        try {
          val attachment = getAttachment(email, i)
          attachmentList.add(attachment)
        } catch {
          case e: Exception => {
            LOGGER.warn(s"Unable to process file attachment due to $e - discarding ${email.getInternetMessageId} attachment ${i}")
          }
        }
      }
      map.put(AttachmentsKey, attachmentList)
    }
  }

  //http://rjohnsondev.github.io/java-libpst/apidocs/com/pff/PSTAttachment.html
  def getAttachment(email: PSTMessage, attachmentIndex: Int): Attachment = {
    val pstAttachment = email.getAttachment(attachmentIndex)
    var attachmentStream: InputStream = null
    try {
      attachmentStream = pstAttachment.getFileInputStream
      val byteBuffer = readToByteBuffer(attachmentStream)
      val attachmentSize = pstAttachment.getAttachSize
      val fileName = {
        val longFileName = pstAttachment.getLongFilename
        if (longFileName.isEmpty()) {
          pstAttachment.getFilename
        } else {
          longFileName
        }
      }
      val mimeType = pstAttachment.getMimeTag
      val attachment = new Attachment()
      attachment.setFileName(fileName)
      attachment.setSize(attachmentSize)
      attachment.setMimeType(mimeType)
      attachment.setData(byteBuffer)
      attachment
    } finally {
      IOUtils.closeQuietly(attachmentStream)
    }
  }

  def readToByteBuffer(attachmentIn: InputStream): ByteBuffer = {
    val pstLibInternalBlockSize = 8176
    val byteOut = new ByteArrayOutputStream(pstLibInternalBlockSize)
    IOUtils.copy(attachmentIn, byteOut)
    ByteBuffer.wrap(byteOut.toByteArray())
  }

  def addRecipients(email: PSTMessage, map: mutable.Map[String, AnyRef]): Unit = {
    val recipientsCount = email.getNumberOfRecipients
    val tos = new java.util.ArrayList[String]()
    val ccs = new java.util.ArrayList[String]()
    val bccs = new java.util.ArrayList[String]()
    for (i <- 0 until recipientsCount) {
      val recipient = email.getRecipient(i)
      val emailAdx = recipient.getSmtpAddress
      recipient.getRecipientType match {
        case PSTRecipient.MAPI_TO  => tos.add(emailAdx)
        case PSTRecipient.MAPI_CC  => ccs.add(emailAdx)
        case PSTRecipient.MAPI_BCC => bccs.add(emailAdx)
      }
    }
    if (tos.size() > 0) {
      map.put(ToKey, tos)
    }
    if (ccs.size() > 0) {
      map.put(CcKey, ccs)
    }
    if (bccs.size() > 0) {
      map.put(BccKey, bccs)
    }
  }

}
