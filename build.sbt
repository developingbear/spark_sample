name := "atom_metic_core"

version := "1.0-SNAPSHOT"

idePackagePrefix := Some("com.kakao.adrec.atom.metric")

// 기존 버전 유지
scalaVersion := "2.12.10"
val sparkVersion = "2.4.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
)

import sbt._

updateOptions := updateOptions.value.withCachedResolution(true)


