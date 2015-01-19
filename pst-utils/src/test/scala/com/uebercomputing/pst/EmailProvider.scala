package com.uebercomputing.pst

import com.pff.PSTFile
import com.pff.PSTFolder
import java.util.Arrays
import scala.collection.JavaConverters._
import com.pff.PSTMessage
import com.pff.PSTObject

trait EmailProvider {

  val TestFilePath = "src/test/resources/psts/enron1.pst"
  val ParentFolders = List("Personal folders", "Top of Personal Folders", "Deleted Items", "rapp-b", "Rapp, Bill (Non-Privileged)", "Rapp, Bill", "Inbox").mkString(PstConstants.ParentFolderSeparator)

  def getPstFile(): PSTFile = {
    val pstFile = new PSTFile(TestFilePath)
    pstFile
  }

  /**
   * Returns PSTMessage email based on param descriptor index from TestFilePath.
   */
  def getEmail(descriptorIndex: Long): PSTMessage = {
    val pstFile = getPstFile()
    val pstObj = PSTObject.detectAndLoadPSTObject(pstFile, descriptorIndex)
    pstObj match {
      case msg: PSTMessage => msg
      case default         => throw new RuntimeException(s"$descriptorIndex did not return PSTMessage but ${default.getClass}")
    }
  }

  /**
   * Personal folders
   * |  |  |- Top of Personal Folders 0
   * |  |  |  |- Deleted Items 0
   * |  |  |  |- rapp-b 0
   * |  |  |  |  |- Rapp, Bill (Non-Privileged) 0
   * |  |  |  |  |  |- Rapp, Bill 0
   * |  |  |  |  |  |  |- hr info 3
   * |  |  |  |  |  |  |- Sent Items 10
   * |  |  |  |  |  |  |- Inbox 56
   */
  def getInboxFolder(): PSTFolder = {
    val pstFile = getPstFile()
    val rootFolder = pstFile.getRootFolder
    val rootfolders = rootFolder.getSubFolders.asScala
    val topOfPersonalFolder = rootfolders(0)
    val personalFolders = topOfPersonalFolder.getSubFolders.asScala
    val rappBFolder = personalFolders(1)
    val nonPrivFolder = rappBFolder.getSubFolders.asScala(0)
    val rappBillFolder = nonPrivFolder.getSubFolders.asScala(0)
    val inboxFolder = rappBillFolder.getSubFolders.asScala(2)
    inboxFolder
  }
}
