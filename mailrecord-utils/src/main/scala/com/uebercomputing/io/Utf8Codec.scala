package com.uebercomputing.io

import java.nio.charset.CodingErrorAction
import scala.io.Codec

/**
 * Standard codec we use for all input/output.
 */
object Utf8Codec {

  // e.g. Source.fromInputStream has implicit codec argument
  // See http://stackoverflow.com/questions/13625024/how-to-read-a-text-file-with-mixed-encodings-in-scala-or-java
  val codec = Codec("UTF-8")
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

}
