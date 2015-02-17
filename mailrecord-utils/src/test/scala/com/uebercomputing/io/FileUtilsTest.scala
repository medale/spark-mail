package com.uebercomputing.io

import com.uebercomputing.test.UnitTest
import java.io.File

class FileUtilsTest extends UnitTest {

  test("getMatchingFilesRecursively non-empty - .txt") {
    val startingDir = new File("src/test/resources")
    val txtFileFilter = new FileExtensionFilter(".txt")
    val results = FileUtils.getMatchingFilesRecursively(startingDir, txtFileFilter)
    //pushd mailrecord-utils/src/test/resources/
    //find . -name *.txt | wc -l
    assert(results.size === 13)
  }

  test("getMatchingFilesRecursively non-empty - .xml") {
    val startingDir = new File("src/test/resources")
    val xmlFileFilter = new FileExtensionFilter(".XML", ".xml")
    val results = FileUtils.getMatchingFilesRecursively(startingDir, xmlFileFilter)
    assert(results.size === 1)
    assert(results(0).getName === "log4j2-test.xml")
  }

  test("getMatchingFilesRecursively empty - .avro") {
    val startingDir = new File("src/test/resources")
    val avroFileFilter = new FileExtensionFilter(".avro", ".AVRO")
    val results = FileUtils.getMatchingFilesRecursively(startingDir, avroFileFilter)
    assert(results.size === 0)
  }

}
