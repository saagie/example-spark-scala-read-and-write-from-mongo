import sbt.Keys._

name := "example-spark-scala-read-and-write-from-mongo"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1" % "provided"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.3.0"
libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "1.0.0"
libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"


assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
