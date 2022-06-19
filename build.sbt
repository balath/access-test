name := "sdg-access-test"

version := "0.1"

scalaVersion := "2.12.16"

val circeVersion = "0.8.0"
val sparkVersion = "3.2.1"
val munitVersion = "0.7.29"

libraryDependencies ++= Seq(
  "org.scalameta" %% "munit" % munitVersion % "test",
  "io.circe" %% "circe-parser" % circeVersion,
  "io.circe" %% "circe-core" % circeVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
