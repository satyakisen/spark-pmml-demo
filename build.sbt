ThisBuild / organization := "org.ssen"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.8"


lazy val root = (project in file(".")).
  settings(
    name := "spark-pmml-demo",
    description := "A Demo project exploring spark pmml",
    assembly / mainClass := Some("org.ssen.Driver")
  )

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.2.2"

