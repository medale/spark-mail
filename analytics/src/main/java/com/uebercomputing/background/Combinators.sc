package com.uebercomputing.background

object Combinators {

  def computeLength(w: String): Int = w.length    //> computeLength: (w: String)Int

  val words = List("when", "shall", "we", "three", "meet", "again")
                                                  //> words  : List[String] = List(when, shall, we, three, meet, again)
  val lengths = words.map(computeLength)          //> lengths  : List[Int] = List(4, 5, 2, 5, 4, 5)
  
  val l2 = words.map((w: String) => w.length)     //> l2  : List[Int] = List(4, 5, 2, 5, 4, 5)
  val l3 = words.map(w => w.length)               //> l3  : List[Int] = List(4, 5, 2, 5, 4, 5)
  val l4 = words.map(_.length)                    //> l4  : List[Int] = List(4, 5, 2, 5, 4, 5)
}