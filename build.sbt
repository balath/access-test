name := "sdg-access-test"

version := "0.1"

scalaVersion := "2.13.8"

val circeVersion = "0.14.1"
val sparkVersion = "3.2.1"
val munitVersion = "0.7.29"

libraryDependencies ++= Seq(
  "org.scalameta" %% "munit" % munitVersion,
  "io.circe" %% "circe-optics" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)