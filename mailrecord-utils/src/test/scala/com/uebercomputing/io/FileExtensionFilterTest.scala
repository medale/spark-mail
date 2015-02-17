package com.uebercomputing.io

import com.uebercomputing.test.UnitTest
import java.io.File

class FileExtensionFilterTest extends UnitTest {

  test("accept tests") {
    val filter = new FileExtensionFilter(".pst", ".PST")
    val extensions = List(".pst", ".PST", ".avro")
    val expecteds = List(true, true, false)
    for ((extension, expected) <- extensions zip expecteds) {
      val path = new File(s"file${extension}")
      assert(filter.accept(path) === expected)
    }
  }
}
