import sbt.Keys._

name := "example-spark-scala-read-and-write-from-mongo"

version := "1.1"

scalaVersion := "2.11.11"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided"
libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.1.0"
libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"


assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
