//sbt plugins - https://dl.bintray.com/sbt/sbt-plugin-releases/
//Note: SBT runs with scala 2.12 - we can use 2.12 for sbt-assembly
//invoke via: sbt assembly
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.9")

//invoke via: sbt scalastyle
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

//invoke via: sbt editsource:edit/clean
addSbtPlugin("org.clapper" %% "sbt-editsource" % "1.0.0")

//invoke via: dependencyTree
//invoke via: whatDependsOn <org> <module> <revision>
//whatDependsOn commons-beanutils commons-beanutils-core 1.8.0
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.2")

//sbt <config-scope>:packageBin
//sbt packageZipTarball
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.3.12")

//sbt avroScalaGenerateSpecific
addSbtPlugin("com.julianpeeters" % "sbt-avrohugger" % "2.0.0-RC14")
