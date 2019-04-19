package com.descriptiveanalytics

import org.apache.spark.sql.SparkSession


object CassandraReader{
 def main(args: Array[String]): Unit ={


   val sparkSession = SparkSession.builder.appName("CassandraReader").
     config("spark.cassandra.connection.host", "18.136.251.110").
     config("spark.cassandra.connection.port", "9042").master("local[1]").getOrCreate()

   import sparkSession.implicits._

   val df = sparkSession
     .read
     .format("org.apache.spark.sql.cassandra")
     .options(Map( "table" -> "stock_twits", "keyspace" -> "bigdata" ))
     .load()

   // print(df.show(20, false))
   // df.collect.foreach(println)
   df.printSchema()

   // filter by the stock symbol
   val aaplDF = df.filter(df("symbol") === "AAPL")

   // create intermediate (dummy) columns to store the date and hour from the created_at field


   // group by dummy_date (text), dummy_hour, entities_sentiment_basic
   aaplDF.groupBy("entities_sentiment_basic").count().show()

   // get aggregates for counts, likes_total, user_followers

 }
}
