package com.uebercomputing.background

/**
 * Used throughout Scala collections, also available on RDDs!
 */
object FunctionalCombinators {

  def main(args: Array[String]): Unit = {
    def computeLength(w: String): Int = w.length

    val words = List("when", "shall", "we", "three", "meet", "again")
    val lengths = words.map(computeLength)

    val list2 = words.map((w: String) => w.length)
    val list3 = words.map(w => w.length)
    val list4 = words.map(_.length)

    val macbeth = """When shall we three meet again?
 		 |In thunder, lightning, or in rain?""".stripMargin

    val macLines = macbeth.split("\n")

    // Non-word character split
    val macWordsNested: Array[Array[String]] = macLines.map { line => line.split("""\W+""") }

    val macWords: Array[String] = macLines.flatMap { line => line.split("""\W+""") }

    val macWordsLower = macWords.map { _.toLowerCase }

    val stopWords = List("in", "it", "let", "no", "or", "the")
    val withoutStopWords = macWordsLower.filter(word => !stopWords.contains(word))

    // beware of overflow if using default Int!
    val numberOfAttachments: List[Long] = List(0, 3, 4, 1, 5)
    val totalAttachments1 = numberOfAttachments.reduce((total, currVal) => {
      println(s"currValue: $currVal and total: $total")
      currVal + total
    })
    val totalAttachments = numberOfAttachments.reduce((x, y) => x + y)

    val emptyList: List[Int] = Nil
    // UnsupportedOperationException
    emptyList.reduce((x, y) => x + y)

    val numbers = List(1, 4, 5, 7, 8, 11)
    val evenCount = numbers.fold(0) { (count, currVal) =>
      println(s"Count: $count, value: $currVal")
      if (currVal % 2 == 0) {
        count + 1
      } else {
        count
      }
    }
    println(s"Even count was $evenCount")

    val str = "this:is:a:string"
    val count = str.foldLeft(0)((count, ch) => if (ch == ':') count + 1 else count)

    val wordsAll =
      List("when", "shall", "we", "three", "meet", "again", "in", "thunder", "lightning", "or", "in", "rain")
    // expected: (4 -> 3), (2 -> 4), (5 -> 3), (7 -> 1), (9 -> 1)
    // actual: Map(5 -> 3, 9 -> 1, 2 -> 4, 7 -> 1, 4 -> 3)
    val lengthDistro = wordsAll.aggregate(Map[Int, Int]())(
      seqop = (distMap, currWord) => {
        val length = currWord.length()
        val newCount = distMap.getOrElse(length, 0) + 1
        val newKv = (length, newCount)
        distMap + newKv
      },
      combop = (distMap1, distMap2) => {
        distMap1 ++ distMap2.map { case (k, v) =>
          (k, v + distMap1.getOrElse(k, 0))
        }
      }
    )
    println(lengthDistro)
  }

}
