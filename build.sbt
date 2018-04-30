name := "Vicki"

version := "0.1"

scalaVersion := "2.11.12"

dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-core" % "2.9.5",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.5",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.5"
)

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.3.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.3.0",
  "org.apache.spark" % "spark-mllib_2.11" % "2.3.0",
  "com.crealytics" % "spark-excel_2.11" % "0.9.15"
)