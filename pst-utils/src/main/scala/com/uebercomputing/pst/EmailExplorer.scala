package com.uebercomputing.pst

import java.io.File
import java.util.Date

import scala.collection.JavaConverters.asScalaBufferConverter

import com.pff.PSTFile
import com.pff.PSTFolder
import com.pff.PSTMessage

/**
 * Focus on one PST file and explore content metadata.
 */
object EmailExplorer {

  val DefaultPst = "/opt/rpm1/jebbush/200401-06JanJun.pst"

  case class Metrics(var totalEmails: Int = 0, var totalReadableEmails: Int = 0, var totalAttachments: Int = 0) {}

  def main(args: Array[String]): Unit = {

    var currFolder = ""

    try {
      val pstPath = if (args.length > 0) args(0) else DefaultPst
      val startTime = new Date()
      println(s"Start time: ${startTime}")

      val pstFile = new PSTFile(pstPath)
      println(s"PST Name:  ${pstFile.getMessageStore().getDisplayName()}")

      val metrics = Metrics()
      processFolders(pstFile.getRootFolder(), metrics)

      val stopTime = new Date()
      println(s"Stop time: ${stopTime}")
      val runtimeSecs = (stopTime.getTime - startTime.getTime) / 1000
      println(s"Ran for ${runtimeSecs}")

      println(metrics)
    } catch {
      case e: Throwable => sys.error(s"Unable to process $currFolder due to $e")
    }
  }

  def processFolders(folder: PSTFolder, metrics: Metrics): Unit = {
    var folderTotal = 0
    val contentCount = folder.getContentCount()
    metrics.totalEmails += contentCount
    if (contentCount > 0) {
      processFolderContent(folder, metrics)
    }

    if (folder.hasSubfolders()) {
      val childFolders = folder.getSubFolders()
      for (childFolder <- childFolders.asScala) {
        processFolders(childFolder, metrics)
      }
    }
  }

  def processFolderContent(folder: PSTFolder, metrics: Metrics): Unit = {
    try {
      var pstObj = folder.getNextChild()
      while (pstObj != null) {
        pstObj match {
          case email: PSTMessage => processEmail(email, metrics)
          case default           => println(s"Found something of type ${default.getClass}")
        }
        pstObj = folder.getNextChild()
      }
    } catch {
      case e: Throwable => println(s"Unable to process folder ${folder.getDisplayName()} due to $e")
    }
  }

  def processEmail(email: PSTMessage, metrics: Metrics): Unit = {
    val getters = email.getClass.getMethods.filter { method =>
      val name = method.getName
      name.startsWith("get")
    }.map { _.getName() }
    println("Getters: " + getters.mkString(", "))
    val filterMethods = List("getRTFBody")
    for (getterName <- getters.diff(filterMethods)) {
      val runtimeUniverse = scala.reflect.runtime.universe
      val runtimeMirror = runtimeUniverse.runtimeMirror(getClass.getClassLoader)
      try {
        val instanceMirror = runtimeMirror.reflect(email)
        val termName = runtimeUniverse.newTermName(getterName)
        val getterMethod = runtimeUniverse.typeOf[PSTMessage].declaration(termName).asMethod
        val getterReflect = instanceMirror.reflectMethod(getterMethod)
        val result = getterReflect()
        println(s"Invoked: $getterName")
        println(s"Result>> $result<<")
      } catch {
        case e: Exception => println(s"Unable to invoke $getterName because of $e")
      }
    }
    /*val attachmentCount = email.getNumberOfAttachments()
    metrics.totalAttachments += attachmentCount
    metrics.totalReadableEmails += 1
    println("Start email>>")
    //println(s"Body>>${email.getBody()}<<")
    println(s"ReplyToId>>${email.getInReplyToId}<<")
    println(s"InternetMessageId>>${email.getInternetMessageId}<<")
    println(s"MessageSize>>${email.getMessageSize}<<")
    println(s"NumberOfAttachments>>${email.getNumberOfAttachments}<<")
    println(s"OriginalSubject>>${email.getOriginalSubject}<<")
    println(s"SenderName>>${email.getSenderName}<<")
    println(s"Subject>>${email.getSubject}<<")
    println(s"EmailAddress>>${email.getEmailAddress}<<")
    println(s"CreationTime>>${email.getCreationTime}<<")
    //println(s">>${email.}<<")
*/
  }

  def printDepth(depth: Int): Unit = {
    for (i <- 0 to depth) {
      print(" | ")
    }
    print(" |- ")
  }
}
