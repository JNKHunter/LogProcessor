resolvers in ThisBuild ++= Seq("Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/", Resolver.mavenLocal)

name := "logprocessor"

version := "0.1-SNAPSHOT"

organization := "brightmeta"

scalaVersion in ThisBuild := "2.11.7"

val flinkVersion = "1.3.0"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-connector-kafka-0.10" % flinkVersion,
  // https://mvnrepository.com/artifact/com.clearspring.analytics/stream
  // https://mvnrepository.com/artifact/com.google.code.gson/gson
  "com.google.code.gson" % "gson" % "2.8.1",
  "com.clearspring.analytics" % "stream" % "2.9.5",

  "junit" % "junit" % "4.12" % "test",
  "org.hamcrest" % "hamcrest-all" % "1.3" % "test")

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies
  )

mainClass in assembly := Some("brightmeta.LogProcessorApp")

// make run command include the provided dependencies
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

// exclude Scala library from assembly
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
