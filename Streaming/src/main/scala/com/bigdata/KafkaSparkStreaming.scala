package com.bigdata

import org.apache.spark.SparkConf
import org.apache.spark.streaming._

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

class KafkaSparkStreaming(val zkQuorum: String, val group: String, val topics: String, val numThreads: String) {
  def run(): Unit = {

    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val streamingContext = new StreamingContext(sparkConf, Seconds(2))
    streamingContext.checkpoint("checkpoint")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("topicA", "topicB")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(record => (record.key, record.value))

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}

object KafkaSparkStreaming extends App {
  if (args.length < 4) {
    System.err.println("Usage: KafkaSparkStreaming <zkQuorum><group> <topics> <numThreads>")
    System.exit(1)
  }

  val Array(zkQuorum, group, topics, numThreads) = args
  val app = new KafkaSparkStreaming(zkQuorum, group, topics, numThreads)
  app.run()
}