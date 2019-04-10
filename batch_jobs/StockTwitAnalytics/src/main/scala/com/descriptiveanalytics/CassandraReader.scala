package com.descriptiveanalytics
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import org.apache.spark._
import org.apache.spark.SparkContext._

object CassandraReader{
 def main(args: Array[String]): Unit ={
   val conf = new SparkConf().setAppName("CassandraReader").setMaster("local[1]")

   val sc = new SparkContext(conf)
   val rdd = sc.cassandraTable("bigdata", "stock_twits")
   println(rdd.count)
   println(rdd.first)
   println(rdd.map(_.getInt("likes_total")).sum)
 }
}
