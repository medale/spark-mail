import sbt._

object Dependencies {

  val sparkVersion = "2.3.2"

  //match Spark's pom for these dependencies!
  val scalaVersionStr = "2.11.8"
  val hadoopVersion = "2.7.3"
  val parquetVersion = "1.8.3"
  val avroVersion = "1.7.7"
  val log4jVersion = "1.2.17"
  //end of Spark version match

  val commonDependencies = Seq(
     ("org.scala-lang" % "scala-library" % scalaVersionStr),
     ("com.uebercomputing" % "sparkmail-mailrecord" % "1.0.0"),
     ("org.apache.avro" % "avro" % avroVersion),
     ("org.apache.parquet" % "parquet-avro" % parquetVersion),
     ("com.twitter" %% "chill-avro" % "0.9.3"),
     ("com.jsuereth" %% "scala-arm" % "2.0"),
     ("com.github.scopt" %% "scopt" % "3.7.0"),
     ("joda-time" % "joda-time" % "2.10.1"),
     ("org.joda" % "joda-convert" % "2.1.2"),
     ("commons-io" % "commons-io" % "2.4"),
     ("log4j" % "log4j" % log4jVersion)
  )

  val sparkDependenciesBase = Seq(
    ("org.apache.spark" %% "spark-core" % sparkVersion)
      .exclude("org.scalatest", "scalatest_2.11"),
    ("org.apache.spark" %% "spark-sql" % sparkVersion)
      .exclude("org.scalatest", "scalatest_2.11"),
    ("org.apache.spark" %% "spark-hive" % sparkVersion)
      .exclude("org.scalatest", "scalatest_2.11"),
    ("org.apache.spark" %% "spark-graphx" % sparkVersion)
      .exclude("org.scalatest", "scalatest_2.11"),
    ("com.databricks" %% "spark-avro" % "4.0.0")
  )

  val sparkDependencies = sparkDependenciesBase.map(_ % "provided")

  //test and integration test dependencies/scope
  val testDependencies = Seq(
    ("org.scalatest" %% "scalatest" % "3.0.5" % "it,test")
  )

  val sparkTestDependencies = Seq(("com.holdenkarau" %% "spark-testing-base" % "2.3.1_0.10.0" % "it,test"))
}
