//sbt plugins - https://central.sonatype.com

// invoke via: whatDependsOn <org> <module> <revision>
// e.g. whatDependsOn commons-beanutils commons-beanutils-core 1.9.4
addDependencyTreePlugin

// invoke via: sbt assembly
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.2.0")

// invoke via: sbt scalafix
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.12.1")

// invoke via: sbt scalafmtAll
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")

// invoke via: sbt packageZipTarball
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.10.0")

//sbt avroScalaGenerateSpecific
addSbtPlugin("com.julianpeeters" % "sbt-avrohugger" % "2.8.3")

// https://github.com/rtimush/sbt-updates
// invoke via: dependencyUpdates
// invoke via: dependencyUpdatesReport (default output target/dependency-updates.txt)
// dependencyUpdatesFilter -= moduleFilter(organization = "org.scala-lang")
addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.6.4")

