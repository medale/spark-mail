package com.uebercomputing.pst

import com.pff.PSTFile
import com.pff.PSTFolder
import com.pff.PSTMessage
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import collection.JavaConverters._

import java.io.File
import java.lang.reflect.Method
import java.lang.reflect.Type
import java.util.ArrayList
import java.util.List
import java.util.Vector
import com.pff.PSTFile
import com.pff.PSTFolder
import com.pff.PSTMessage
import java.io.File
import java.io.FileFilter

@RunWith(classOf[JUnitRunner])
class PstApiTest extends FunSuite {

  val PstHome = "/opt/rpm1/jebbush"

  class PstFileFilter extends FileFilter {
    def accept(path: File): Boolean = {
      val name = path.getName
      name.endsWith(".pst")
    }
  }

  def processFolder(folder: PSTFolder, depth: Int): Unit = {
    if (depth > 0) {
      //root folder at depth 0 has no display name
      printDepth(depth)
      print(folder.getDisplayName())
      val contentCount = folder.getContentCount()
      println(s" $contentCount")
      if (contentCount > 0) {
        processFolderContent(folder, depth + 1)
      }
    }

    if (folder.hasSubfolders()) {
      val childFolders = folder.getSubFolders()
      for (childFolder <- childFolders.asScala) {
        processFolder(childFolder, depth + 1)
      }
    }
  }

  def processFolderContent(folder: PSTFolder, depth: Int): Unit = {
    try {
      var pstObj = folder.getNextChild()
      while (pstObj != null) {
        pstObj match {
          case email: PSTMessage => //processEmail(email, depth)
          case default           => println(s"Found something of type ${default.getClass}")
        }
        pstObj = folder.getNextChild()
      }
    } catch {
      case e: Throwable => println(s"Unable to process folder ${folder.getDisplayName()} due to $e")
    }
  }

  def processEmail(email: PSTMessage, depth: Int): Unit = {
    printDepth(depth)
    println(email.getSubject())
  }

  def printDepth(depth: Int): Unit = {
    for (i <- 0 to depth) {
      print(" | ")
    }
    print(" |- ")
  }

  test("Use PST api...") {
    val baseDir = new File(PstHome)
    val pstPaths = baseDir.listFiles(new PstFileFilter())
    pstPaths.foreach { pstPath =>
      val pstFile = new PSTFile(pstPath)
      println(pstFile.getMessageStore().getDisplayName())
      processFolder(pstFile.getRootFolder(), 0)
    }
  }
}

//    // hasAttachments()
//    //
//    private String getInfo(PSTMessage email) throws Exception {
//        Method[] methods = email.getClass().getMethods();
//        List<Method> getMethods = new ArrayList<Method>();
//        for (Method method : methods) {
//            String methodName = method.getName();
//            if (methodName.startsWith("get")) {
//                getMethods.add(method);
//            }
//        }
//        StringBuilder builder = new StringBuilder();
//        for (Method method : getMethods) {
//            Type[] paramTypes = method.getGenericParameterTypes();
//            if (paramTypes.length == 0) {
//                Object result = method.invoke(email);
//                builder.append("\n" + method.getName() + ": ");
//                builder.append(result);
//            }
//        }
//        return builder.toString();
//    }
