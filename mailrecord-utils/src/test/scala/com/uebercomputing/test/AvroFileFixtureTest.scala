package com.uebercomputing.test

import org.scalatest.fixture.FunSuite

class AvroFileFixtureTest extends FunSuite with AvroMailRecordsFileProvider {

  type FixtureParam = AvroFileTestInfo

  var testInfo: AvroFileTestInfo = _

  override def withFixture(test: OneArgTest) = {
    try {
      testInfo = createAndOpenTestFile()
      test(testInfo)
    } finally {
      closeAndDeleteTestFile(testInfo)
    }
  }
}
