package com.uebercomputing.io

import scala.annotation.tailrec
import java.io.FileFilter
import java.io.File

object FileUtils {

  def getMatchingFilesRecursively(startingDir: File, fileFilter: FileFilter): List[File] = {

    @tailrec def getMatchingFilesRecursively(dirs: List[File], files: List[File], fileFilter: FileFilter): List[File] = {
      def getDirsAndMatchingFiles(dir: File): (List[File], List[File]) = {
        val potentialFiles = dir.listFiles().toList
        val (dirs, files) = potentialFiles.partition { file =>
          file.isDirectory()
        }
        val matchingFiles = files.filter { file =>
          fileFilter.accept(file)
        }
        (dirs, matchingFiles)
      }

      dirs match {
        case dir :: otherDirs => {
          val (newDirs, matchingFiles) = getDirsAndMatchingFiles(dir)
          getMatchingFilesRecursively(otherDirs ++ newDirs, files ++ matchingFiles, fileFilter)
        }
        case List(dir) => {
          val (newDirs, matchingFiles) = getDirsAndMatchingFiles(dir)
          getMatchingFilesRecursively(newDirs, files ++ matchingFiles, fileFilter)
        }
        case Nil => files
      }
    }

    getMatchingFilesRecursively(List(startingDir), Nil, fileFilter)
  }

}
