package com.uebercomputing.mailparser

import com.uebercomputing.test.UnitTest
import java.io.FileInputStream
import org.apache.commons.io.IOUtils
import scala.io.Source
import scala.util.control.NonFatal

class MessageParserTest extends UnitTest {

  val TestFileWithLongTo = "/enron/maildir/kean-s/deleted_items/338."

  test("parseRaw for multiline Subject") {
    val subject = """Subject: RE: Alliant Energy - IES Utilities dispute re: Poi 2870 - Cherokee
                  |#1 TBS - July 99 thru April 2001""".stripMargin
    val expected = Map("Subject" -> "RE: Alliant Energy - IES Utilities dispute re: Poi 2870 - Cherokee #1 TBS - July 99 thru April 2001")

    assertResult(expected) {
      val lines = Source.fromString(subject).getLines()
      MessageParser.parseRaw(lines)
    }
  }

  test("parseRaw for header with empty value should return empty string value") {
    val message = "X-cc:"
    assertResult(Map("X-cc" -> "")) {
      val lines = Source.fromString(message).getLines()
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
      |""".stripMargin

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
      "Body" -> "No, Alliant only agreed to pay.  I am still working with Dari and Shelly on a letter to be sent.  Thanks,  Raetta")

    val lines = Source.fromString(message).getLines()
    val actual = MessageParser.parseRaw(lines)

    for {
      (key, value) <- expected
    } {
      assert(actual.contains(key))
      assert(value === actual(key))
    }

  }

  test("parse actual mail message from file") {
    val msgUrl = "/enron/maildir/lay-k/inbox/898."
    var msgStream = getClass().getResourceAsStream(msgUrl)
    var msgSource: Option[Source] = None
    var actual: Map[String, String] = Map.empty
    try {
      msgSource = Some(Source.fromInputStream(msgStream))
      actual = MessageParser(msgSource.get)
    } catch {
      case NonFatal(ex) => println(s"Non fatal exception! $ex")
    } finally {
      for (s <- msgSource) {
        s.close
      }
    }
    assert(actual("Body").startsWith("The following"))
    assert(actual("X-FileName") === "klay (Non-Privileged).pst")
    assert(actual("To") === "babbio@verizon.com, j58391@aol.com, ghh@telcordia.com, kenneth.lay@enron.com, slitvack@deweyballantine.com, kjewett@kpcb.com, lsalhany@lifefx.com")
    assert(actual.size === 16)
  }

  test("process failing email with long 'To' section") {
    val in = getClass().getResourceAsStream(TestFileWithLongTo)
    val msg = IOUtils.toString(in)
    val mailIn = Source.fromString(msg)
    val map = MessageParser(mailIn)
    val expectedRaw = "/o=enron/ou=na/cn=recipients/cn=notesaddr/cn=f21d9b15-25189ad0-8625653f-482bf6@enron.com, " +
      "lynnette.barnes@enron.com, london.brown@enron.com, " +
      "janet.butler@enron.com, guillermo.canovas@enron.com, " +
      "stella.chan@enron.com, shelley.corman@enron.com"
    val to = map("To")
    assert(to.split(",").map(_.trim).toSet === expectedRaw.split(",").map(_.trim).toSet)
    IOUtils.closeQuietly(in)
  }

  ignore("process 163") {
    //fixed by implicit utf-8 codec - add specific test!
    //TODO
    val in = new FileInputStream("/opt/rpm1/enron/enron_mail_20110402/maildir/kean-s/california___working_group/163.")
    val msg = IOUtils.toString(in)
    val mailIn = Source.fromString(msg)
    val map = MessageParser(mailIn)
    println(map)
    IOUtils.closeQuietly(in)
  }

}
