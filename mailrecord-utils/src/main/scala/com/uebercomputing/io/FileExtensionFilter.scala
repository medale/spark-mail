package com.uebercomputing.io

import java.io.FileFilter
import java.io.File

class FileExtensionFilter(extensions: String*) extends FileFilter {

  def accept(path: File): Boolean = {
    val name = path.getName
    extensions.foldLeft(false)((resultSoFar, currSuffix) => resultSoFar || name.endsWith(currSuffix))
  }
}
