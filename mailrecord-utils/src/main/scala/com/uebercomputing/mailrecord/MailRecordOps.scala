package com.uebercomputing.mailrecord

import java.util.{List => JavaList}
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import scala.jdk.CollectionConverters._

object MailRecordOps {

  /**
   * Converts Scala values to Java MailRecord implementation generated from Avro avdl.
   *
   * @param uuid
   * @param from
   * @param tosOpt
   * @param ccsOpt
   * @param bccsOpt
   * @param dateUtcEpoch
   * @param subject
   * @param mailFieldsOpt
   * @param body
   * @param attachmentsOpt
   * @return
   *   new MailRecord with param values
   */
  def apply(
      uuid: String,
      from: String,
      tosOpt: Option[Seq[String]] = None,
      ccsOpt: Option[Seq[String]] = None,
      bccsOpt: Option[Seq[String]] = None,
      dateUtcEpoch: Long,
      subject: String,
      mailFieldsOpt: Option[Map[String, String]] = None,
      body: String,
      attachmentsOpt: Option[Seq[Attachment]] = None
    ): MailRecord = {

    val builder = MailRecord.newBuilder()
    builder.setUuid(uuid)
    builder.setFrom(from)
    tosOpt.foreach(tos => builder.setTo(tos.asJava))
    ccsOpt.foreach(ccs => builder.setCc(ccs.asJava))
    bccsOpt.foreach(bccs => builder.setBcc(bccs.asJava))
    builder.setDateUtcEpoch(dateUtcEpoch)
    builder.setSubject(subject)
    mailFieldsOpt.foreach(mailFields => builder.setMailFields(mailFields.asJava))
    builder.setBody(body)
    attachmentsOpt.foreach(attachments => builder.setAttachments(attachments.asJava))

    val mailRecord = builder.build()
    mailRecord
  }

}

case class MailRecordOps(mailRecord: MailRecord) {

  def getAttachmentsOpt(): Option[List[Attachment]] = {
    val attachments = mailRecord.getAttachments
    getAsScalaListOption(attachments)
  }

  def getToOpt(): Option[List[String]] = {
    val to = mailRecord.getTo
    getAsScalaListOption(to)
  }

  def getCcOpt(): Option[List[String]] = {
    val cc = mailRecord.getCc
    getAsScalaListOption(cc)
  }

  def getBccOpt(): Option[List[String]] = {
    val bcc = mailRecord.getBcc
    getAsScalaListOption(bcc)
  }

  def getMailFieldsAsScala(): Map[String, String] = {
    val mailFields = mailRecord.getMailFields
    val mutableMap = mailFields.asScala
    mutableMap.toMap
  }

  def getMailFieldOpt(key: String): Option[String] = {
    val mailFields = mailRecord.getMailFields
    Option(mailFields.get(key))
  }

  def getDateUtc(): DateTime = {
    val dateUtcEpoch = mailRecord.getDateUtcEpoch
    new DateTime(dateUtcEpoch, DateTimeZone.UTC)
  }

  /**
   * Returns Some[scala.collection.immutable.List[T]] if javaList is not null, None otherwise (thanks, Jeff!)
   */
  private def getAsScalaListOption[T](javaList: JavaList[T]): Option[List[T]] = {
    Option(javaList).map { java =>
      java.asScala.toList
    }
  }

}
