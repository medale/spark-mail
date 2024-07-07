import Dependencies._

addCommandAlias("fix", "all compile:scalafix test:scalafix; scalafmtAll")

name := "spark-mail"

// https://scalacenter.github.io/scalafix/docs/users/installation.html
inThisBuild(
  List(
    scalaVersion := "2.13.14", 
    semanticdbEnabled := true, // enable SemanticDB
    semanticdbVersion := scalafixSemanticdb.revision // only required for Scala 2.x
  )
 )

// http://docs.oracle.com/javase/17/docs/technotes/tools/windows/javac.html
// -Xlint enable all recommended warnings
ThisBuild / javacOptions ++= Seq("-source", "17", "-target", "17", "-Xlint", "-encoding", "UTF-8")
// https://github.com/ThoughtWorksInc/sbt-best-practice/blob/master/scalac-options/src/main/scala/com/thoughtworks/sbtBestPractice/scalacOptions/ScalacWarnings.scala
// scalac -help
// -unchecked Enable detailed unchecked (erasure) warnings
// -deprecation Emit warning and location for usages of deprecated APIs.
// -feature Emit warning and location for usages of features that should be imported explicitly. (e.g. postfix)
ThisBuild / scalacOptions := Seq("-unchecked", "-deprecation", "-feature", "-Xlint:infer-any", "-Wunused:imports")

//the following settings according to https://github.com/holdenk/spark-testing-base
ThisBuild / Test / javaOptions ++= Seq("-Xms8G", "-Xmx8G")
ThisBuild / Test / parallelExecution := false

ThisBuild / initialize := {
  val _ = initialize.value
  if (sys.props("java.specification.version") != "17") {
    sys.error("Java 17 is required for this project.")
  }
}

// for scaladoc to link to external libraries (see https://www.scala-sbt.org/1.x/docs/Howto-Scaladoc.html)
ThisBuild / autoAPIMappings := true

ThisBuild / resolvers += Resolver.mavenLocal

// https://github.com/sbt/sbt-assembly
ThisBuild / Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated
ThisBuild / Compile / runMain := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated

// run sbt-assembly via: sbt assembly to build fat jar
lazy val assemblyPluginSettings = Seq(
  assembly / assemblyJarName := s"${baseDirectory.value.name}-${version.value}-fat.jar",
  //exclude scala from generated fat jars
  assembly / assemblyOption ~= { _.withIncludeScala(false) },
  //http://queirozf.com/entries/creating-scala-fat-jars-for-spark-on-sbt-with-sbt-assembly-plugin
  assembly / assemblyMergeStrategy := {
    case PathList("javax", "ws", _@_*) => MergeStrategy.discard
    case PathList("javax", "servlet", _@_*) => MergeStrategy.discard
    case PathList(html@_*) if html.last endsWith ".html" => MergeStrategy.discard
    case PathList(xs@_*) if ((xs.head == "META-INF") && (xs.last.endsWith(".SF"))) => MergeStrategy.discard
    case PathList(xs@_*) if ((xs.head == "META-INF") && (xs.last.endsWith(".DSA"))) => MergeStrategy.discard
    case PathList(xs@_*) if ((xs.head == "META-INF") && (xs.last.endsWith(".RSA"))) => MergeStrategy.discard
    case PathList(xs@_*) if ((xs.head == "META-INF") && (xs.last == "org.apache.hadoop.fs.FileSystem")) => MergeStrategy.concat
    case "log4j.properties" => MergeStrategy.discard
    case "log4j2.xml" => MergeStrategy.discard
    case "application.conf" => MergeStrategy.concat
    case "META-INF/io.netty.versions.properties" => MergeStrategy.concat
    case "META-INF/versions/9/module-info.class" => MergeStrategy.first
    case "mime.types" => MergeStrategy.first
    case x =>
      val oldStrategy = (assembly / assemblyMergeStrategy).value
      oldStrategy(x)
  }
)

// group name for publishing artifacts
lazy val miscSettings = Seq(
  organization := "com.uebercomputing"
)

lazy val combinedSettings = miscSettings ++ assemblyPluginSettings

////////////////////////////////////////////////////////////////////////////
// Project/module definitions                                             //
////////////////////////////////////////////////////////////////////////////

lazy val mailrecordUtils = (project in file("mailrecord-utils"))
  .settings(combinedSettings: _*)
  .settings(
    libraryDependencies :=
      commonDependencies ++
      hadoopDependencies ++
      sparkDependencies ++
        testDependencies
  )

lazy val analyticsBaseDir = "analytics"

lazy val datasetAnalytics = (project in file(s"${analyticsBaseDir}/dataset"))
  .dependsOn(mailrecordUtils)
  .settings(combinedSettings: _*)
  .settings(
    libraryDependencies :=
      commonDependencies ++
        sparkDependencies ++
        testDependencies ++
        sparkTestDependencies
  )

lazy val rddAnalytics = (project in file(s"${analyticsBaseDir}/rdd"))
.dependsOn(mailrecordUtils)
.settings(combinedSettings: _*)
.settings(
  libraryDependencies :=
    commonDependencies ++
      sparkDependencies ++
      testDependencies ++
      sparkTestDependencies
)

// root just needs to aggregate all projects
// when we use any sbt command it gets run for all subprojects also
lazy val root = (project in file("."))
  .aggregate(mailrecordUtils, datasetAnalytics, rddAnalytics)
  .settings(publish := {})
  .settings(assemblyPluginSettings: _*)
