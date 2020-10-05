import sbt._
import sbt.Keys._

name := "data_engineering_superstore"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0.cloudera1" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.1.0.cloudera1" % "provided",
  "com.github.nscala-time" %% "nscala-time" % "1.6.0",
  "com.typesafe" % "config" % "1.3.0",
  "com.holdenkarau" %% "spark-testing-base" % "2.1.0_0.9.0" % "test", 
  "org.apache.spark" %% "spark-hive"  % "2.1.0" % "test"
)

resolvers ++= Seq(
  "cloudera" at "http://repository.cloudera.com/artifactory/cloudera-repos/"
)

scalacOptions ++= Seq("-unchecked", "-deprecation")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// Testing options
fork in Test := false
parallelExecution in Test := false
test in assembly := {}

