name := "StockTwits-Cassandra-Spark"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

libraryDependencies += "io.spray" %%  "spray-json" % "1.3.5"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % sparkVersion