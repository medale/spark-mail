package com.uebercomputing.pst

import com.pff.PSTMessage

import scala.collection.mutable
import java.util.UUID

object PstEmailToMapProcessor {
  val MsgId = "Message-ID"
  val Date = "Date"
  val From = "From"
  val To = "To"
  val Cc = "Cc"
  val Bcc = "Bcc"
  val Subject = "Subject"
  val Body = "Body"
  val InternetMessageId = "InternetMessageId"
}

/**
 *
 */
class PstEmailToMapProcessor {

  import PstEmailToMapProcessor._

  /**
   * record MailRecord {
   * string uuid;
   * string from;
   * union{array<string>,null} to = null;
   * union{array<string>,null} cc = null;
   * union{array<string>,null} bcc = null;
   * long dateUtcEpoch;
   * string subject;
   * union{map<string>,null} mailFields = null;
   * string body;
   * }
   * Can include String, List[String], Date
   */
  def process(email: PSTMessage): mutable.Map[String, AnyRef] = {
    val map = mutable.Map[String, AnyRef]()
    map(MsgId) = UUID.randomUUID().toString()
    map(From) = email.getSenderEmailAddress
    map(Date) = email.getMessageDeliveryTime //Date object
    map(Subject) = email.getSubject //String
    map(Body) = email.getBody //String
    map(InternetMessageId) = email.getInternetMessageId //String
    addAttachmentsIfPresent(email, map)
    map
  }

  def addAttachmentsIfPresent(email: PSTMessage, map: mutable.Map[String, AnyRef]): Unit = {
    val attachmentCount = email.getNumberOfAttachments
    //TODO - add attachments to map
  }

  def addTos(email: PSTMessage, map: mutable.Map[String, AnyRef]): Unit = {
    val recipientsCount = email.getNumberOfRecipients
    println(recipientsCount)
    email.getRecipient(0)
  }
}
