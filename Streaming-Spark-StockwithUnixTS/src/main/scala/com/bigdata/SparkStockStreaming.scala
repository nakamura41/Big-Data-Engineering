package com.bigdata

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scala.util.Try
import spray.json._
import org.json4s._
import org.json4s.native.JsonMethods._

object StreamingExample extends Logging {
  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      logInfo("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}

class SparkStreamingWithUnixTS() {

  val localLogger = Logger.getLogger("SparkStreamingWithUnixTS")

  def getDouble(value: JValue): Double = {
    value match {
      case JNull => 0
      case JNothing => 0
      case JDouble(i) => i.doubleValue()
    }
  }

  def getInteger(value: JValue): Int = {
    value match {
      case JNull => 0
      case JNothing => 0
      case JInt(i) => i.intValue()
    }
  }

  def getString(value: JValue): String = {
    value match {
      case JNull => ""
      case JNothing => ""
      case JString(i) => i
    }
  }


  def run(brokers: String, groupId: String, topics: String): Unit = {

    StreamingExample.setStreamingLogLevels()

    val sparkConf = new SparkConf().setMaster("local[*]").set("spark.cassandra.connection.host", "18.136.251.110")
      .set("spark.cassandra.connection.port", "9042").setAppName("SparkStreamingWithUnixTS")

    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint("checkpoint")
    val connector = CassandraConnector(sparkConf)

    localLogger.info("brokers:" + brokers)
    localLogger.info("groupId:" + groupId)
    localLogger.info("topics :" + topics)

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])

    val events = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
      .map((record: ConsumerRecord[String, String]) => {
        record.value
      }).foreachRDD(rdd => rdd.foreach(json =>
      connector.withSessionDo(session => {
        val jsonObj = parse(json)
        System.out.println(json)
        val jsonDate = (jsonObj \ "date").values
        val jsonMinute = (jsonObj \ "minute").values
        val jsonHigh = (jsonObj \ "high").values
        val jsoncombine = jsonDate.toString + jsonMinute.toString
        System.out.println(jsonDate)
        System.out.println(jsonMinute)
        System.out.println(jsonHigh)
        val sourceFormat = DateTimeFormat.forPattern("yyyyMMddhh:mm")
        val targetFormat = DateTimeFormat.forPattern("yyyy-MM-dd hh:mm")
        //val prepared = session.prepare("INSERT INTO bigdata.stock_quote_batch JSON '" + json.toString + "'")


        def parsed(str: String): DateTime = {
          DateTime.parse(str,sourceFormat)
        }
        val test_output = parsed(jsoncombine.toString)
        println (test_output.getMillis / 10000)
        //session.execute(prepared.bind())
      })

    ))



    //    messagesDS.map((record: ConsumerRecord[String, String]) => {
    //      val stockRecord = record.value.parseJson.asJsObject()
    //    }).saveToCassandra("bigdata", "stock_quote", SomeColumns(""))

    //    lines.saveToCassandra("bigdata", "stock_quote")

    //    lines.foreachRDD { rdd =>
    //      rdd.foreach { record =>
    //        System.out.println(record.getFields("companyName"))
    //      }
    //    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}

object SparkStreamingWithUnixTS extends App {
  if (args.length < 3) {
    System.err.println(
      s"""
         |Usage: KafkaStockStreaming <brokers> <groupId> <topics>
         |  <brokers> is a list of one or more Kafka brokers
         |  <groupId> is a consumer group name to consume from topics
         |  <topics> is a list of one or more kafka topics to consume from
         |
         |Example: KafkaStockStreaming localhost:9020 group1 stock-quote
        """.stripMargin)
    System.exit(1)
  }

  val app = new SparkStreamingWithUnixTS()
  app.run(args(0), args(1), args(2))
}