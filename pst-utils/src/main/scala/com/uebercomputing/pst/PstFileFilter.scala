package com.uebercomputing.pst

import java.io.FileFilter
import java.io.File

class PstFileFilter extends FileFilter {
  def accept(path: File): Boolean = {
    val name = path.getName
    name.endsWith(".pst")
  }
}
