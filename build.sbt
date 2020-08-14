name := "customer-shopping-patterns"

version := "0.1"

scalaVersion := "2.12.11"

val sparkVersion = "2.4.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.typesafe" % "config" % "1.3.0",
  "org.apache.directory.studio" % "org.apache.commons.io" % "2.4" % Test,
  "org.scalatest" %% "scalatest" % "3.0.0" % Test
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

