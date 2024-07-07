import sbt._

object Dependencies {

  val sparkVersion = "3.5.1"

  //match Spark's pom for these dependencies!
  val scalaVersionStr = "2.13.14"
  val hadoopVersion = "3.3.4"
  val parquetVersion = "1.13.1"
  val avroVersion = "1.11.2"
  val log4jVersion = "2.20.0"
  //end of Spark version match

  val commonDependencies = Seq(
     ("org.scala-lang" % "scala-library" % scalaVersionStr),
     ("org.apache.avro" % "avro" % avroVersion),
     ("org.apache.parquet" % "parquet-avro" % parquetVersion),
     ("com.github.scopt" %% "scopt" % "4.1.0"),
     ("commons-io" % "commons-io" % "2.13.0"),
     ("org.apache.logging.log4j" % "log4j-slf4j2-impl" % log4jVersion)
  )

  val hadoopDependencies = Seq(
    ("org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided")
      .exclude("commons-beanutils", "commons-beanutils")
      .exclude("commons-beanutils", "commons-beanutils-core")
      .exclude("com.fasterxml.jackson.core", "jackson-core")
      .exclude("com.fasterxml.jackson.core", "jackson-annotations")
      .exclude("com.fasterxml.jackson.core", "jackson-databind")
      .exclude("org.slf4j", "slf4j-api")
  )

  //Avro - https://spark-packages.org/
  val sparkDependenciesBase = Seq(
    ("org.apache.spark" %% "spark-core" % sparkVersion)
      .exclude("org.scalatest", "scalatest_2.13"),
    ("org.apache.spark" %% "spark-sql" % sparkVersion)
      .exclude("org.scalatest", "scalatest_2.13"),
    ("org.apache.spark" %% "spark-hive" % sparkVersion)
      .exclude("org.scalatest", "scalatest_2.13"),
    ("org.apache.spark" %% "spark-graphx" % sparkVersion)
      .exclude("org.scalatest", "scalatest_2.13"),
    ("org.apache.spark" %% "spark-avro" % "3.5.1")
  )

  val sparkDependencies = sparkDependenciesBase.map(_ % "provided")

  //test and integration test dependencies/scope
  val testDependencies = Seq(
    ("org.scalatest" %% "scalatest" % "3.2.19" % "test")
  )

  val sparkTestDependencies = Seq(("com.holdenkarau" %% "spark-testing-base" % "3.5.1_1.5.3" % "test"))
}
