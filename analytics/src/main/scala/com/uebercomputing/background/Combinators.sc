package com.uebercomputing.background

object Combinators {

  def computeLength(w: String): Int = w.length

  val words = List("when", "shall", "we", "three", "meet", "again")
  val lengths = words.map(computeLength)
  
  val l2 = words.map((w: String) => w.length)
  val l3 = words.map(w => w.length)
  val l4 = words.map(_.length)
  
  val macbeth = """|When shall we three meet again?
 		 |In thunder, lightning, or in rain?""".stripMargin
 		 
  val macLines = macbeth.split("\n")
  
  //Non-word character split
  val macWordsNested: Array[Array[String]] = macLines.map{line => line.split("""\W+""")}
  
  val macWords: Array[String] = macLines.flatMap{line => line.split("""\W+""")}
  
  val stopWords = List("in","it","let","no","the","too")
  val withoutStopWords = macWords.filter(word => !stopWords.contains(word))
}