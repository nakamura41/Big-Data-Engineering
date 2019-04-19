package com.descriptiveanalytics

import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Date
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.from_unixtime


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


   aaplDF
     .withColumn("created_at_hour", from_unixtime($"created_at"/1000, "HH"))
     .withColumn("created_at_date", from_unixtime($"created_at"/1000, "yyyy-MM-dd"))
     .show()

   //val dateConverterExpr = "new Date(created_at)"

   // create intermediate (dummy) columns to store the date and hour from the created_at field
   //val d = new Date(1513714678 * 1000L)

   //val getDate: Double => String = Date(_).getTime()

   //aaplDF.withColumn("created_at_date", _ => new Date($"created_at")).show()


   // group by dummy_date (text), dummy_hour, entities_sentiment_basic
   // aaplDF.groupBy("entities_sentiment_basic").count().show()

   // get aggregates for counts, likes_total, user_followers


   // save the results in the new table in Cassandra
   /*
   val dfprev = aaplDF.select("se","hu")
   dfprev.write.format("org.apache.spark.sql.cassandra")
     .options(Map("keyspace"->"bigdata","table"->"stock_twits_aggregate"))
     .mode(SaveMode.Append)
     .save()
     */

 }
}
