package com.uebercomputing.mailparser

import java.lang.RuntimeException

case class ParseException(msg: String) extends RuntimeException {

}
