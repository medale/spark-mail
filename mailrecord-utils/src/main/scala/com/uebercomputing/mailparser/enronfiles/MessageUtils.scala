package com.uebercomputing.mailparser.enronfiles

import org.joda.time.format.DateTimeFormat

import scala.util.control.NonFatal
import com.google.common.hash.Hashing
import com.google.common.base.Charsets

import scala.util.Failure
import scala.util.Success
import scala.util.Try

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
    val hashString = Hashing.md5().hashString(message, Charsets.UTF_8)
    val md5Hash = hashString.toString()
    md5Hash
  }
}
