package com
import org.apache.spark
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext


class Archival() {

  def run(): Unit = {
    val sparkSession = SparkSession.builder.appName("CassandraReader").
      config("spark.cassandra.connection.host", "18.136.251.110").
      config("spark.cassandra.connection.port", "9042").master("local[1]").getOrCreate()

    import sparkSession.implicits._

    val df_stockquotes = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "stock_twits", "keyspace" -> "bigdata"))
      .load()

    val df_stocktwits = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "stock_quote_batch", "keyspace" -> "bigdata"))
      .load()

    import java.time.Instant
    val curr_unixTimestamp = Instant.now.getEpochSecond
    val ten_years_earlier = curr_unixTimestamp - 315360000
    //    val ten_years_earlier = curr_unixTimestamp

    //    println(ten_years_earlier)
    val df_month_year_stockqoutes = df_stockquotes
      //      .withColumn("month_created", from_unixtime(df_stockquote("created_at"), "MM"))
      //      .withColumn("year_created", from_unixtime($"created_at", "yyyy"))
      .withColumn("date_unix", from_unixtime($"created_at"))
    val df_month_year_stockqoutes_unix = df_month_year_stockqoutes
      .withColumn("U_TS", unix_timestamp($"date_unix"))

    val df_month_year_stocktwits = df_stocktwits
      .withColumn("date_unix", from_unixtime($"created_at"))
    val df_month_year_stocktwits_unix = df_month_year_stocktwits
      .withColumn("U_TS", unix_timestamp($"date_unix"))

    //    val sc: SparkContext
    //    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //    sqlContext.setConf(“spark.sql.parquet.compression.codec”, “snappy”)
    //    import sqlContext.implicits._

    val df_for_archival_stockqoutes = df_month_year_stockqoutes_unix.filter($"U_TS" <= ten_years_earlier)
    df_for_archival_stockqoutes.write.option("compression", "snappy").mode(SaveMode.Append).parquet("hdfs://sparknode01.localdomain:9000/archival/stockqoutes/")

    val df_for_archival_stocktwits = df_month_year_stocktwits_unix.filter($"U_TS" <= ten_years_earlier)
    df_for_archival_stocktwits.write.option("compression", "snappy").mode(SaveMode.Append).parquet("hdfs://sparknode01.localdomain:9000/archival/stocktwits/")

  }
}

object Archival extends App {

  val app = new Archival()
  app.run()

}