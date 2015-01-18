package com.uebercomputing.pst

import scala.collection.mutable
import com.uebercomputing.mailrecord.MailRecord
import java.util.Date
import com.uebercomputing.mailrecord.Attachment

object PstEmailMapToMailRecordConverter {

  import PstEmailToMapProcessor._

  private val mbr = MailRecord.newBuilder()

  /**
   * Takes information from map param and converts it into a new MailRecord
   * object. (TODO: check object creation overhead - should we reuse?)
   */
  def toMailRecord(map: mutable.Map[String, AnyRef]): MailRecord = {
    resetMailRecordBuilderOptionalFieldsAndMap()
    mbr.setUuid(map(UuidKey).asInstanceOf[String])
    mbr.setFrom(map(FromKey).asInstanceOf[String])
    map.get(ToKey).flatMap { value =>
      mbr.setTo(value.asInstanceOf[java.util.List[String]])
      None
    }
    map.get(CcKey).flatMap { value =>
      mbr.setCc(value.asInstanceOf[java.util.List[String]])
      None
    }
    map.get(BccKey).flatMap { value =>
      mbr.setBcc(value.asInstanceOf[java.util.List[String]])
      None
    }
    mbr.setDateUtcEpoch(map(DateKey).asInstanceOf[Date].getTime)
    mbr.setSubject(map(SubjectKey).asInstanceOf[String])
    addMailFields(mbr.getMailFields, map)
    mbr.setBody(map(BodyKey).asInstanceOf[String])
    map.get(AttachmentsKey).flatMap { value =>
      mbr.setAttachments(value.asInstanceOf[java.util.List[Attachment]])
      None
    }
    mbr.build()
  }

  def addMailFields(mailFieldsMap: java.util.Map[String, String], map: mutable.Map[String, AnyRef]): Unit = {
    val mailFieldsKeys = List(DescriptorNodeIdKey, InternetMessageIdKey, ParentFoldersKey, OriginalPstPathKey)
    for (key <- mailFieldsKeys) {
      map.get(key).flatMap { value =>
        val valStr = value.asInstanceOf[String]
        mailFieldsMap.put(key, valStr)
        None
      }
    }
  }

  private def resetMailRecordBuilderOptionalFieldsAndMap(): Unit = {
    mbr.clearTo()
    mbr.clearCc()
    mbr.clearBcc()
    val mailFieldsMap: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    mbr.setMailFields(mailFieldsMap)
    mbr.clearAttachments()
  }
}
