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

  val UuidKey = "Uuid"
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
  def process(uuid: String, email: PSTMessage, originalPstPath: String, parentFolders: String): mutable.Map[String, AnyRef] = {
    val map = mutable.Map[String, AnyRef]()
    map(UuidKey) = uuid
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
            LOGGER.warn(s"Unable to process file attachment ${i} in ${email.getDescriptorNodeId} in ${map(OriginalPstPathKey)} due to ${e}")
          }
        }
      }
      map.put(AttachmentsKey, attachmentList)
    }
  }

  //http://rjohnsondev.github.io/java-libpst/apidocs/com/pff/PSTAttachment.html
  def getAttachment(email: PSTMessage, attachmentIndex: Int): Attachment = {
    var attachmentStream: InputStream = null
    try {
      val pstAttachment = email.getAttachment(attachmentIndex)
      attachmentStream = pstAttachment.getFileInputStream
      val (byteBuffer, attachmentSize) = readToByteBuffer(attachmentStream)
      val fileName = {
        val longFileName = pstAttachment.getLongFilename.trim()
        if (longFileName.isEmpty()) {
          pstAttachment.getFilename.trim()
        } else {
          longFileName
        }
      }
      val mimeType = pstAttachment.getMimeTag.trim()
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

  def readToByteBuffer(attachmentIn: InputStream): (ByteBuffer, Int) = {
    val pstLibInternalBlockSize = 8176
    val byteOut = new ByteArrayOutputStream(pstLibInternalBlockSize)
    val bytesCopied = IOUtils.copy(attachmentIn, byteOut)
    val byteBuffer = ByteBuffer.wrap(byteOut.toByteArray())
    (byteBuffer, bytesCopied)
  }

  def addRecipients(email: PSTMessage, map: mutable.Map[String, AnyRef]): Unit = {
    val recipientsCount = email.getNumberOfRecipients
    val tos = new java.util.ArrayList[String]()
    val ccs = new java.util.ArrayList[String]()
    val bccs = new java.util.ArrayList[String]()
    for (i <- 0 until recipientsCount) {
      try {
        val recipient = email.getRecipient(i)
        val emailAdx = recipient.getSmtpAddress
        recipient.getRecipientType match {
          case PSTRecipient.MAPI_TO  => tos.add(emailAdx)
          case PSTRecipient.MAPI_CC  => ccs.add(emailAdx)
          case PSTRecipient.MAPI_BCC => bccs.add(emailAdx)
          case default => {
            LOGGER.warn(s"Unknown recipient type $default. Storing recipients in To field.")
            tos.add(emailAdx)
          }
        }
      } catch {
        case e: Exception => LOGGER.warn(s"Unable to get recipient ${i} in ${email.getDescriptorNodeId} in ${map(OriginalPstPathKey)} due to ${e}")
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
