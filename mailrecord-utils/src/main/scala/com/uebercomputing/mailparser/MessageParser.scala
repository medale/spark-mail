package com.uebercomputing.mailparser

import scala.io.Source

object MessageParser {

  val MsgId = "Message-ID"
  val Date = "Date"
  val From = "From"
  val To = "To"
  val Cc = "Cc"
  val Bcc = "Bcc"
  val XTo = "X-To"
  val XCc = "X-Cc"
  val XBcc = "X-Bcc"
  val Subject = "Subject"
  val Body = "Body"

  val endOfHeaderMarker = ""
  val space = " "

  private val keyValueRE = """(.*?): (.*)""".r

  def apply(src: Source): Map[String, AnyRef] = {
    val lines = src.getLines
    Map.empty
  }

  def parseRaw(lines: Iterator[String]): Map[String, String] = {

    def parseBody(lines: Iterator[String], messageParts: Map[String, String]): Map[String, String] = {
      val body = lines.mkString(space)
      messageParts.updated(Body, body)
    }

    // format: OFF
    def parseRawHelper(lines: Iterator[String], messageParts: Map[String, String], lastKey: Option[String]):
      Map[String, String] = {
   // format: ON
      if (lines.hasNext) {
        val line = lines.next().trim()
        line match {
          case endOfHeader if line == endOfHeaderMarker => parseBody(lines, messageParts)
          case keyValueRE(key, value) => {
            val lastKey = Some(key)
            val isBody = false
            parseRawHelper(lines, messageParts.updated(key, value), lastKey)
          }
          case multiLineHeader => {
            lastKey match {
              case Some(key) => {
                val previousValue = messageParts(key)
                val newValue = previousValue + space + line
                val isBody = false
                parseRawHelper(lines, messageParts.updated(key, newValue), lastKey)
              }
              case None => throw ParseException("Found line matching multiheader line but no previous header key or body")
            }
          }
        }
      } else {
        messageParts
      }
    }
    parseRawHelper(lines, Map.empty[String, String], None)
  }
}
