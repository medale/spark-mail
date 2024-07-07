package com.uebercomputing.test

import org.scalatest.Outcome
import org.scalatest.funsuite.FixtureAnyFunSuite

class AvroFileFixtureTest extends FixtureAnyFunSuite with AvroMailRecordsFileProvider {

  type FixtureParam = AvroFileTestInfo

  var testInfo: AvroFileTestInfo = _

  override def withFixture(test: OneArgTest): Outcome = {
    try {
      testInfo = createAndOpenTestFile()
      test(testInfo)
    } finally {
      closeAndDeleteTestFile(testInfo)
    }
  }

}
