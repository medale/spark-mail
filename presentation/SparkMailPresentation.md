% Learning Apache Spark by processing Enron Email
% Markus Dale
% 2015

# Talk Overview

# Speaker Background

# Combinator functions on Scala collections

* Examples: map, flatMap, filter
* Background - Combinatory logic, higher-order functions...

# Combinatory Logic

* Moses Schönfinkel and Haskell Curry in the 1920s

> [C]ombinator is a higher-order function that uses only function application and earlier defined combinators to define a result from its arguments [Combinatory Logic @wikipedia_combinatory_2014]

# Higher-order function
* Takes function as argument or returns function

# map - explicit function

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

# map - Scala Docs
```
final def
map[B](f: (A) ⇒ B): List[B]

[use case]
Builds a new collection by applying a function to all elements of this list.
B
the element type of the returned collection.
f
the function to apply to each element.
returns
a new list resulting from applying the given function f to each element of this list and collecting the results.
```

# Spark RDD
* Resilient Distributed Dataset
```scala
val lines = sc.textFile("test.txt")
val wc = lines.map(...
```

# References {.allowframebreaks}
