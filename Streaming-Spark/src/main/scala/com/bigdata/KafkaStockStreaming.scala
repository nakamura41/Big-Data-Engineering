package com.bigdata

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import spray.json._

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

class KafkaStockStreaming() {

  val localLogger = Logger.getLogger("KafkaStockStreaming")

  def run(brokers: String, groupId: String, topics: String): Unit = {

    StreamingExample.setStreamingLogLevels()

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaStockStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
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
        record.value.parseJson.toString
      }).foreachRDD(rdd => rdd.foreach(json =>
      connector.withSessionDo(session => {
        System.out.println(json)
        val prepared = session.prepare("INSERT INTO bigdata.stock_quote JSON '" + json.toString + "'")
        session.execute(prepared.bind())
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

object KafkaStockStreaming extends App {
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

  val app = new KafkaStockStreaming()
  app.run(args(0), args(1), args(2))
}