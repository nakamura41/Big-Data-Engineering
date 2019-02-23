package com.bigdata

import java.util.Properties

import kafka.utils.Logging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import scalaj.http._

class SimpleProducer(val topic: String) extends Logging {

  val props: Properties = createProducerConfig()
  val producer = new KafkaProducer[String, String](props)

  def createProducerConfig(): Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.RETRIES_CONFIG, 0)
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384)
    props.put(ProducerConfig.LINGER_MS_CONFIG, 1)
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props
  }

  def run(stockTicker: String): Unit = {
    val topic: String = this.topic
    val timestamp: Long = System.currentTimeMillis
    val stockUrl: String = s"https://api.iextrading.com/1.0/stock/$stockTicker/quote"
    val stockId: String = s"$stockTicker-$timestamp"
    val response: HttpResponse[String] = Http(stockUrl)
      .header("Content-Type", "application/json")
      .header("Charset", "UTF-8")
      .option(HttpOptions.readTimeout(10000)).asString
    System.out.println(s"Publish $stockTicker stock quote: id $stockId")
    producer.send(new ProducerRecord[String, String](topic, stockId, response.body))
    System.out.println("Message sent successfully")
    producer.close()
  }

}

object SimpleProducer extends App {
  if (args.length == 2) {
    val app = new SimpleProducer(args(0))
    app.run(args(1))
  } else {
    System.out.println("Enter topic name and stock ticker")
  }
}
