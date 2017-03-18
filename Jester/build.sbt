name := "Jester"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1",
  "com.databricks" % "spark-csv_2.10" % "1.4.0",
  "org.apache.spark" %% "spark-sql" % "1.6.1",
  "org.apache.spark" %% "spark-mllib" % "1.6.1",
  "org.slf4j" % "slf4j-api" % "1.7.10",
  "org.scala-lang" % "scala-compiler" % "2.10.6",
  "org.scala-lang" % "scala-reflect" % "2.10.6",
  "org.apache.commons" % "commons-lang3" % "3.3.2"
)

