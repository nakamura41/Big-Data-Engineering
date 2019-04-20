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
import java.util.HashMap
import org.json4s._
import org.json4s.native.JsonMethods._
import com.google.gson.Gson

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
        //        System.out.println(json)

        val jsonDate = (jsonObj \ "date").values
        val jsonMinute = (jsonObj \ "minute").values
        val jsonHigh = (jsonObj \ "high").values

        val jsoncombine = jsonDate.toString + jsonMinute.toString

        val jsonlabel = (jsonObj \ "label").values
        val jsonhigh = (jsonObj \ "high").values
        val jsonlow = (jsonObj \ "low").values
        val jsonaverage = (jsonObj \ "average").values
        val jsonvol = (jsonObj \ "volume").values
        val jsonnotion = (jsonObj \ "notional").values
        val jsonNoTrades = (jsonObj \ "numberOfTrades").values
        val jsonMarketHigh = (jsonObj \ "marketHigh").values
        val jsonMarketLow = (jsonObj \ "marketLow").values
        val jsonMarketAvg = (jsonObj \ "marketAverage").values
        val jsonMarketVol = (jsonObj \ "marketVolume").values
        val jsonMarketNotion = (jsonObj \ "marketNotional").values
        val jsonMarketNoTrades = (jsonObj \ "marketNumberOfTrades").values
        val jsonOpen = (jsonObj \ "open").values
        val jsonClose = (jsonObj \ "close").values
        val JsonMarketOpen = (jsonObj \ "marketOpen").values
        val JsonMarketClose = (jsonObj \ "marketClose").values
        val JsonChangeTime = (jsonObj \ "changeOverTime").values
        val JsonMarketChangeTime = (jsonObj \ "marketChangeOverTime").values


        //        System.out.println(jsonDate)
        //        System.out.println(jsonMinute)
        //        System.out.println(jsonHigh)
        val sourceFormat = DateTimeFormat.forPattern("yyyyMMddHH:mm")
        val targetFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")
        //val prepared = session.prepare("INSERT INTO bigdata.stock_quote_batch JSON '" + json.toString + "'")


        def parsed(str: String): DateTime = {
          DateTime.parse(str, sourceFormat)
        }

        val epoch_time = parsed(jsoncombine.toString).getMillis
        println(epoch_time)

        val jsonDoc = new HashMap[String, Any]()
        jsonDoc.put("symbol", "AAPL")
        jsonDoc.put("created_at", epoch_time)
        jsonDoc.put("id", 0)
        jsonDoc.put("companyName", "Apple")
        jsonDoc.put("sector", "Information Technology")
        jsonDoc.put("date", jsonDate.toString)
        jsonDoc.put("minute", jsonMinute.toString)
        jsonDoc.put("label", jsonlabel.toString)
        jsonDoc.put("high", jsonhigh.toString.toDouble)
        jsonDoc.put("low", jsonlow.toString.toDouble)
        jsonDoc.put("average", jsonaverage.toString.toDouble)
        jsonDoc.put("volume", jsonvol.toString.toDouble)
        jsonDoc.put("notional", jsonnotion.toString.toDouble)
        jsonDoc.put("numberOfTrades", jsonNoTrades.toString.toDouble)
        jsonDoc.put("marketHigh", jsonMarketHigh.toString.toDouble)
        jsonDoc.put("marketLow", jsonMarketLow.toString.toDouble)
        jsonDoc.put("marketAverage", jsonMarketAvg.toString.toDouble)
        jsonDoc.put("marketVolume", jsonMarketVol.toString.toDouble)
        jsonDoc.put("marketNotional", jsonMarketNotion.toString.toDouble)
        jsonDoc.put("marketNumberOfTrades", jsonMarketNoTrades.toString.toDouble)
        jsonDoc.put("open", jsonOpen.toString.toDouble)
        jsonDoc.put("close", jsonClose.toString.toDouble)
        jsonDoc.put("marketOpen", JsonMarketOpen.toString.toDouble)
        jsonDoc.put("marketClose", JsonMarketClose.toString.toDouble)
        jsonDoc.put("changeOverTime", JsonChangeTime.toString.toDouble)
        jsonDoc.put("marketChangeOverTime", JsonMarketChangeTime.toString.toDouble)

        val gson = new Gson
        val jsonString: String = gson.toJson(jsonDoc)

        val prepared = session.prepare(s"INSERT INTO bigdata.stock_quote_batch JSON '$jsonString'")
        session.execute(prepared.bind())
      }
      )))

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