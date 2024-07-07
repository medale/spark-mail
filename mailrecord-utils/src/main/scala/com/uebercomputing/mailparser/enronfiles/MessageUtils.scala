package com.uebercomputing.mailparser.enronfiles

import org.apache.commons.codec.digest.DigestUtils
import org.joda.time.format.DateTimeFormat
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

object MessageUtils {

  private val DateFormatter = DateTimeFormat.forPattern("EEE, dd MMM yyyy HH:mm:ss Z (z)").withZoneUTC()

  def parseDateAsUtcEpochTry(dateStr: String): Try[Long] = {
    try {
      Success(DateFormatter.parseMillis(dateStr))
    } catch {
      case NonFatal(e) => Failure(new ParseException(s"Bad date: $dateStr", e))
    }
  }

  def parseCommaSeparated(commaSeparated: String): List[String] = {
    commaSeparated.split(",").toList
  }

  def getMd5Hash(message: String): String = {
    val md5HashHex = DigestUtils.md5Hex(message)
    md5HashHex
  }

}
