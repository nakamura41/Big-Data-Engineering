name := "StockTwitAnalytics"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.6.2"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.0"