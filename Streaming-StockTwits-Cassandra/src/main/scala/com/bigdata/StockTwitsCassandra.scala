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

class StockTwitsCassandra() {

  val localLogger = Logger.getLogger("StockTwitsCassandra")

  def run(brokers: String, groupId: String, topics: String, cassandraIPAddress: String): Unit = {

    StreamingExample.setStreamingLogLevels()

    val sparkConf = new SparkConf().setMaster("local[2]").
      set("spark.cassandra.connection.host", cassandraIPAddress).setAppName("StockTwitsCassandra")

    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint("checkpoint")
    val connector = CassandraConnector(sparkConf)

    localLogger.warn("brokers:" + brokers)
    localLogger.warn("groupId:" + groupId)
    localLogger.warn("topics :" + topics)
    localLogger.warn("cassandraIPAddress :" + cassandraIPAddress)

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
        val prepared = session.prepare("INSERT INTO bigdata.stock_twits JSON '" + json.toString + "'")
        session.execute(prepared.bind())
      })

    ))
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}

object StockTwitsCassandra extends App {
  if (args.length < 4) {
    System.err.println(
      s"""
         |Usage: StockTwitsCassandra <brokers> <groupId> <topics>
         |  <brokers> is a list of one or more Kafka brokers
         |  <groupId> is a consumer group name to consume from topics
         |  <topics> is a list of one or more kafka topics to consume from
         |  <cassandraIPAddress> is the cassandra IP adddress
         |
         |Example: StockTwitsCassandra localhost:9020 group1 stock-twits 127.0.0.1
        """.stripMargin)
    System.exit(1)
  }

  val app = new StockTwitsCassandra()
  app.run(args(0), args(1), args(2), args(3))
}