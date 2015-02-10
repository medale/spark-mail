package com.uebercomputing.background

object Combinators {

  def main(args: Array[String]): Unit = {
    def computeLength(w: String): Int = w.length

    val words = List("when", "shall", "we", "three", "meet", "again")
    val lengths = words.map(computeLength)

    val list2 = words.map((w: String) => w.length)
    val list3 = words.map(w => w.length)
    val list4 = words.map(_.length)

    val macbeth = """|When shall we three meet again?
 		 |In thunder, lightning, or in rain?""".stripMargin

    val macLines = macbeth.split("\n")

    //Non-word character split
    val macWordsNested: Array[Array[String]] = macLines.map { line => line.split("""\W+""") }

    val macWords: Array[String] = macLines.flatMap { line => line.split("""\W+""") }

    val macWordsLower = macWords.map { _.toLowerCase }

    val stopWords = List("in", "it", "let", "no", "or", "the")
    val withoutStopWords = macWordsLower.filter(word => !stopWords.contains(word))
  }
}
