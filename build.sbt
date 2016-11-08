name := "sql"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.0.1",
  "org.apache.spark" % "spark-hive_2.11" % "2.0.1",
  "org.apache.hbase" % "hbase-client"% "1.2.3",
  "org.apache.hbase" % "hbase-common"% "1.2.3",
  "org.apache.hbase" % "hbase"% "1.2.3",

  "org.scala-lang" % "scala-xml" % "2.11.0-M4",
  "com.typesafe" % "config" % "1.3.0",
  "com.typesafe.akka" % "akka-actor_2.11" % "2.4.0",
  "com.typesafe.akka" % "akka-remote_2.11" % "2.4.0",
  "com.typesafe.akka" % "akka-slf4j_2.11" % "2.4.0"
)