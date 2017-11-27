name := "search"

version := "1.0"

scalaVersion := "2.11.1"
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.0.0" % "provided",
  "org.apache.spark" % "spark-sql_2.11" % "2.0.0",
  "mysql" % "mysql-connector-java" % "5.1.40",
  "io.spray" %%  "spray-json" % "1.3.2",
  "org.elasticsearch" % "elasticsearch-spark-20_2.11" % "5.6.4"
)
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }