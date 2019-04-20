package com
import java.util.Calendar
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext


object Archival {
  def main(args: Array[String]): Unit ={


    val sparkSession = SparkSession.builder.appName("CassandraReader").
      config("spark.cassandra.connection.host", "18.136.251.110").
      config("spark.cassandra.connection.port", "9042").master("local[1]").getOrCreate()

    import sparkSession.implicits._

    val df_stockquote = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "stock_quote_batch", "keyspace" -> "bigdata" ))
      .load()


    import java.time.Instant
    val curr_unixTimestamp = Instant.now.getEpochSecond
    val ten_years_earlier = curr_unixTimestamp - 315360000
//    val ten_years_earlier = curr_unixTimestamp

    println(ten_years_earlier)
    val df_month_year = df_stockquote
      .withColumn("month_created", from_unixtime(df_stockquote("created_at"), "MM"))
      .withColumn("year_created", from_unixtime($"created_at", "yyyy"))
      .withColumn("date_unix", from_unixtime($"created_at"))
    val df_month_year_unix = df_month_year
      .withColumn("U_TS", unix_timestamp($"date_unix"))

    val sc: SparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df_for_archival = df_month_year_unix.filter($"U_TS" <= ten_years_earlier)

    import sqlContext.implicits._

    sqlcontext.setConf(“spark.sql.parquet.compression.codec”, “snappy”)
    df_for_archival.write.format("parquet").mode("Append").option("compression","s").parquet('hdfs://sparknode01.localdomain:9000/archival'))



  }
}
