ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "Project3",
    version := "0.1.0",
    scalaVersion := "2.12.15",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.3.0" % "compile",
      "org.apache.spark" %% "spark-sql" % "3.3.0" % "compile",
      "org.scala-lang" % "scala-reflect" % "2.12.15",
      "org.scala-lang" % "scala-compiler" % "2.12.15"
    )
  )
