package com.bigdata

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

class KafkaStockStreaming() {
  def run(zkQuorum: String, group: String, topics: String, numThreads: String): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

object KafkaStockStreaming extends App {
  if (args.length == 4) {
    val app = new KafkaStockStreaming()
    app.run(args(0), args(1), args(2), args(3))
  } else {
    System.err.println("Usage  : KafkaWordCount <zkQuorum> <group> <topics> <numThreads>\nExample: KafkaWordCount localhost:9092 group1 stock-quote 2")
  }
}