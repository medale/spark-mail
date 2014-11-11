package com.uebercomputing.mailparser

import com.uebercomputing.test.UnitTest
import scala.io.Source

class MessageParserTest extends UnitTest {

  test("parseRaw for multiline Subject") {
    val subject = """Subject: RE: Alliant Energy - IES Utilities dispute re: Poi 2870 - Cherokee
                  |#1 TBS - July 99 thru April 2001""".stripMargin
    println(subject)
    val expected = Map("Subject" -> "RE: Alliant Energy - IES Utilities dispute re: Poi 2870 - Cherokee #1 TBS - July 99 thru April 2001")

    assertResult(expected) {
      val lines = Source.fromString(subject).getLines()
      MessageParser.parseRaw(lines)
    }
  }

  // scalastyle:off
  // newlines get replaced by space in parser for multiline headers!
  test("parseRaw for whole mail message") {
    val message = """Message-ID: <19546475.1075853053633.JavaMail.evans@thyme>
      |Date: Mon, 17 Sep 2001 11:30:03 -0700 (PDT)
      |From: chris.sebesta@enron.com
      |To: raetta.zadow@enron.com, lynn.blair@enron.com, kathy.washington@enron.com,
      |  dan.fancler@enron.com
      |Subject: RE: Alliant Energy - IES Utilities dispute re: Poi 2870 - Cherokee
      | #1 TBS - July 99 thru April 2001
      |Mime-Version: 1.0
      |Content-Type: text/plain; charset=us-ascii
      |Content-Transfer-Encoding: 7bit
      |X-From: Sebesta, Chris </O=ENRON/OU=NA/CN=RECIPIENTS/CN=CSEBEST2>
      |X-To: Zadow, Raetta </O=ENRON/OU=NA/CN=RECIPIENTS/CN=Rzadow>, Blair, Lynn </O=ENRON/OU=NA/CN=RECIPIENTS/CN=Lblair>, Washington, Kathy </O=ENRON/OU=NA/CN=RECIPIENTS/CN=Kwashin>, Fancler, Dan </O=ENRON/OU=NA/CN=RECIPIENTS/CN=Dfancle>
      |X-cc:
      |X-bcc:
      |X-Folder: \LBLAIR (Non-Privileged)\Blair, Lynn\Acctg - Customer Issues on Billing
      |X-Origin: Blair-L
      |X-FileName: LBLAIR (Non-Privileged).pst
      |
      |No, Alliant only agreed to pay.
      |
      |I am still working with Dari and Shelly on a letter to be sent.
      |
      |Thanks,
      |
      |Raetta
      |
      |    """.stripMargin

    val expected = Map(
      "Message-ID" -> "<19546475.1075853053633.JavaMail.evans@thyme>",
      "Date" -> "Mon, 17 Sep 2001 11:30:03 -0700 (PDT)",
      "From" -> "chris.sebesta@enron.com",
      "To" -> "raetta.zadow@enron.com, lynn.blair@enron.com, kathy.washington@enron.com, dan.fancler@enron.com",
      "Subject" -> "RE: Alliant Energy - IES Utilities dispute re: Poi 2870 - Cherokee #1 TBS - July 99 thru April 2001",
      "Mime-Version" -> "1.0",
      "Content-Type" -> "text/plain; charset=us-ascii",
      "Content-Transfer-Encoding" -> "7bit",
      "X-From" -> "Sebesta, Chris </O=ENRON/OU=NA/CN=RECIPIENTS/CN=CSEBEST2>",
      "X-To" -> "Zadow, Raetta </O=ENRON/OU=NA/CN=RECIPIENTS/CN=Rzadow>, Blair, Lynn </O=ENRON/OU=NA/CN=RECIPIENTS/CN=Lblair>, Washington, Kathy </O=ENRON/OU=NA/CN=RECIPIENTS/CN=Kwashin>, Fancler, Dan </O=ENRON/OU=NA/CN=RECIPIENTS/CN=Dfancle>",
      "X-cc" -> "",
      "X-bcc" -> "",
      "X-Folder" -> """\LBLAIR (Non-Privileged)\Blair, Lynn\Acctg - Customer Issues on Billing""",
      "X-Origin" -> "Blair-L",
      "X-FileName" -> "LBLAIR (Non-Privileged).pst",
      "Body" -> "No, Alliant only agreed to pay.  I am still working with Dari and Shelly on a letter to be sent.  Thanks,  Raetta      ")

    assertResult(expected.toSet) {
      val lines = Source.fromString(message).getLines()
      MessageParser.parseRaw(lines).toSet
    }

  }
}
