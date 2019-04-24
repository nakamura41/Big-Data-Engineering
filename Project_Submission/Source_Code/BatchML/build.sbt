name := "BatchML"

version := "0.1"

scalaVersion := "2.11.8"
val sparkVersion = "2.3.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % sparkVersion