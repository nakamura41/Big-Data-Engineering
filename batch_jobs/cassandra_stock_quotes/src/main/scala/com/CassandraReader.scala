package com

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_unixtime
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
object CassandraReader{


  def main(args: Array[String]): Unit ={
    val sparkSession = SparkSession.builder.appName("CassandraReader").
      config("spark.cassandra.connection.host", "18.136.251.110").
      config("spark.cassandra.connection.port", "9042").master("local[1]").getOrCreate()

    import sparkSession.implicits._

    val sysTime = System.currentTimeMillis / 1000
    val df = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "stock_quote_batch", "keyspace" -> "bigdata" ))
      .load()

    val filteredDf = df.filter(df("symbol") === "AAPL")
    val agg_df = filteredDf
                .withColumn("hour", from_unixtime($"created_at", "HH"))
                .withColumn("date", from_unixtime($"created_at", "yyyy-MM-dd"))


    val agg_stats =  agg_df.groupBy("date", "hour")
      .agg( count($"marketVolume").alias("num_of_transactions"),
            sum($"marketVolume").alias("market_volume_sum"),
            sum($"marketAverage").alias("market_average_sum"),
            sum($"marketNumberOfTrades").alias("market_no_of_trades_sum"))
      .withColumn("id", monotonically_increasing_id())
      .withColumn("symbol", lit("AAPL"))
      .withColumn("job_at", lit(sysTime))

    agg_stats.
      write.
      format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "bigdata", "table" -> "stock_quote_aggregates"))
      .save()

  }

}
