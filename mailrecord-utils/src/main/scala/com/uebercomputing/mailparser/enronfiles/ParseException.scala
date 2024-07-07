package com.uebercomputing.mailparser.enronfiles

case class ParseException(msg: String) extends RuntimeException(msg) {

  def this(msg: String, e: Throwable) = {
    this(msg)
    this.initCause(e)
  }
}
