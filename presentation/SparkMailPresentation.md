% Learning Apache Spark by processing Email
% Markus Dale
% 2015

# Talk Overview

# Speaker Background

# Why Apache Spark?
* High-level, scalable processing framework
* Hadoop MapReduce is very low-level
    * map phase - (internal shuffle/sort) - reduce phase
    * Progammer expresses logic in map/reduce

# Hadoop Ecosystem
* Extremely rich and robust
* SQL interface: Hive
* DSLs: Pig, Cascading/Scalding, Crunch...
* Graph processing: Giraph
* Real-time streaming: Storm
* Machine Learning: Apache Mahout
* Downside: installation, maintenance, cognitive load

# Apache Spark
* Scala, Java, Python APIs
        * Rich combinator functions on RDD abstraction (Resilient Distributed Dataset)
* Spark SQL
* GraphX
* Spark Streaming
* MLlib

# Combinator functions on Scala collections

* Examples: map, flatMap, filter
* Background - Combinatory logic, higher-order functions...

# Combinatory Logic

* Moses Schönfinkel and Haskell Curry in the 1920s

> [C]ombinator is a higher-order function that uses only function application and earlier defined combinators to define a result from its arguments [Combinatory Logic @wikipedia_combinatory_2014]

# Higher-order function
* Takes function as argument or returns function

# map

* Applies a given function to every element of a collection
* Returns collection of output of that function
* input argument - same type as collection type
* return type - can be any type

```scala
def computeLength(w: String): Int = w.length

val words = List("when", "shall", "we", "three", "meet", "again")
val lengths = words.map(computeLength)

> lengths  : List[Int] = List(4, 5, 2, 5, 4, 5)
```

# map - Scala syntactic sugar
```scala
//anonymous function (specifying input arg type)
val l2 = words.map((w: String) => w.length)
> l2  : List[Int] = List(4, 5, 2, 5, 4, 5)

//let compiler infer arguments type
val l3 = words.map(w => w.length)
> l3  : List[Int] = List(4, 5, 2, 5, 4, 5)

//use positionally match argument
val l4 = words.map(_.length)
> l4  : List[Int] = List(4, 5, 2, 5, 4, 5)
```

# map - ScalaDoc

See [immutable List ScalaDoc](http://www.scala-lang.org/api/2.10.4/index.html#scala.collection.immutable.List)
```scala
final def map[B](f: (A) ⇒ B): List[B]
```
```
Builds a new collection by applying a function to all elements of this list.

B - the element type of the returned collection.
f - the function to apply to each element.
returns - a new list resulting from applying the given function f to each
          element of this list and collecting the results.
```

# flatMap

* ScalaDoc:
```scala
def flatMap[B](f: (A) ⇒ GenTraversableOnce[B]): List[B]
```

* [GenTraversableOnce](http://www.scala-lang.org/api/2.10.4/index.html#scala.collection.GenTraversableOnce) - List, Array, Option...
        * can be empty or None
* flatMap takes each element in the GenTraversableOnce and puts it in
order to output List[B]
        * removes inner nesting - flattens
        * output list can be smaller or empty (if intermediates were empty)

# flatMap Example
```scala
val macbeth = """|When shall we three meet again?
|In thunder, lightning, or in rain?""".stripMargin

val macLines = macbeth.split("\n")

//Non-word character split
val macWordsNested: Array[Array[String]] = macLines.map{line => line.split("""\W+""")}

val macWords: Array[String] = macLines.flatMap{line => line.split("""\W+""")}
macWords: Array[String] = Array(When, shall, we, three, meet, again, In, thunder, lightning, or, in, rain)
```

# filter
```scala
def filter(p: (A) ⇒ Boolean): List[A]
```
```
Selects all elements of this list which satisfy a predicate.
returns - a new list consisting of all elements of this list that satisfy the
          given predicate p. The order of the elements is preserved.

# filter Example
```scala
val macWordsLower = macWords.map{_.toLowerCase}
macWordsLower: Array[String] = Array(when, shall, we, three, meet, again, in, thunder, lightning, or, in, rain)

val stopWords = List("in","it","let","no","the","too")
val withoutStopWords = macWordsLower.filter(word => !stopWords.contains(word))
withoutStopWords: Array[String] = Array(when, shall, we, three, meet, again, thunder, lightning, or, rain)
```

# Spark RDD
* Resilient Distributed Dataset
```scala
val lines = sc.textFile("test.txt")
val wc = lines.map(...
```

# References {.allowframebreaks}
