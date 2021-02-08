name := "Simple Project"

version := "1.0"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
    "org.sellmerfud" %% "optparse" % "2.2",
    "org.apache.spark" %% "spark-sql" % "3.0.1" % "provided"
)
