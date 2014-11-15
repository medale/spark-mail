package com.uebercomputing.mailparser

import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter

import scala.collection.JavaConverters._

object MessageUtils {

  private val DateFormatter = DateTimeFormat.forPattern("EEE, dd MMM yyyy HH:mm:ss Z (z)").withZoneUTC()

  def parseDateAsUtcEpoch(dateStr: String): Long = {
    DateFormatter.parseMillis(dateStr)
  }

  def parseCommaSeparated(commaSeparated: String): java.util.List[CharSequence] = {
    commaSeparated.split(",").toList.map(_.asInstanceOf[CharSequence]).asJava
  }
}
