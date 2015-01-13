package com.uebercomputing.mailparser.enronfiles

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

  val EndOfHeaderMarker = ""
  val Space = " "
  val EmptyString = ""

  private val emptyKeyRE = """(.*?):[\s]*""".r
  private val keyValueRE = """(.*?): (.*)""".r
  private val mailAddressHeaders = List(To,Cc,Bcc)

  /**
   * Returns parsed mail message as key/value pairs. mailAddressHeaders are
   * potentially comma-separated mail addresses (if more than one addressee).
   */
  def apply(src: Source): Map[String, String] = {
    val lines = src.getLines
    parseRaw(lines)
  }

  /**
   * Returns a map containing parsed message headers using the original header
   * used in mail message. For example, the line "Subject: Test" would create
   * a key "Subject" and a value of "Test".
   *
   * Some headers may take up multiple lines. We create one line key/value pairs
   * by replacing the newline with a space. A blank line denotes the end of the
   * header section and all subsequent lines are stored under the key "Body".
   */
  def parseRaw(lines: Iterator[String]): Map[String, String] = {

    def parseBody(lines: Iterator[String], messageParts: Map[String, String]): Map[String, String] = {
      val body = lines.mkString(Space)
      messageParts.updated(Body, body)
    }

    //scalariform makes lines too long for scalastyle!
    // format: OFF
    def parseRawHelper(lines: Iterator[String], messageParts: Map[String, String], lastKey: Option[String]):
      Map[String, String] = {
   // format: ON
      if (lines.hasNext) {
        val line = lines.next().trim()
        line match {
          case endOfHeader if line == EndOfHeaderMarker => parseBody(lines, messageParts)
          case keyValueRE(key, value) => {
            val lastKey = Some(key)
            parseRawHelper(lines, messageParts.updated(key, value), lastKey)
          }
          case emptyKeyRE(key) => {
            val lastKey = Some(key)
            val value = EmptyString
            parseRawHelper(lines, messageParts.updated(key, value), lastKey)
          }
          case multiLineHeader => {
            lastKey match {
              case Some(key) => {
                val previousValue = messageParts(key)
                val newValue = previousValue + Space + line
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
