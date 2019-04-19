package com.descriptiveanalytics

import org.apache.spark.sql.{SaveMode, SparkSession}

import org.apache.spark.sql.functions.from_unixtime

import org.apache.spark.sql.functions._


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
   val aaplDFWithDummyCols = aaplDF
     .withColumn("hour", from_unixtime($"created_at"/1000, "HH"))
     .withColumn("date", from_unixtime($"created_at"/1000, "yyyy-MM-dd"))

   // aaplDFWithDummyCols.show()
   val sysTime = System.currentTimeMillis / 1000

   // group by dummy_date (text), dummy_hour, entities_sentiment_basic
   // get aggregates for counts, likes_total, user_followers
   val aaplDFWithAggregatedCols = aaplDFWithDummyCols.groupBy("date", "hour", "entities_sentiment_basic")
     .agg(
       count($"entities_sentiment_basic").alias("num_tweets"),
       sum($"likes_total").alias("total_likes"),
       sum($"user_followers").alias("total_followers"))
     .na.fill(0, Array("num_tweets", "total_likes", "total_followers"))
     .na.fill("Neutral", Array("entities_sentiment_basic"))
     .withColumn("id", monotonically_increasing_id())
     .withColumn("symbol", lit("AAPL"))
     .withColumn("job_at", lit(sysTime))

   aaplDFWithAggregatedCols.show()
   aaplDFWithAggregatedCols.printSchema()

   // save the results in the new table in Cassandra
   aaplDFWithAggregatedCols.
     write.
     format("org.apache.spark.sql.cassandra")
     .options(Map("keyspace" -> "bigdata", "table" -> "stock_twits_aggregate"))
     .save()

 }
}
