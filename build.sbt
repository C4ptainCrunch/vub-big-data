name := "bd-project"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.0.0"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.0.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.0.0"
libraryDependencies += "org.apache.spark" % "spark-graphx_2.11" % "2.0.0"

//
//libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "provided"
//libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "provided"
//libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "provided"
//libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "provided"
//libraryDependencies += "org.apache.spark" % "spark-graphx_2.11" % "provided"
