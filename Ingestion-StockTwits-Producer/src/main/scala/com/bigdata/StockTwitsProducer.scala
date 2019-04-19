package com.bigdata

import java.util.Properties

import kafka.utils.Logging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import play.api.libs.json._
import scalaj.http._

class StockTwitsProducer() extends Logging {

  val props: Properties = createProducerConfig()
  val producer = new KafkaProducer[String, String](props)

  def createProducerConfig(): Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.RETRIES_CONFIG, "0")
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384")
    props.put(ProducerConfig.LINGER_MS_CONFIG, "1")
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props
  }

  def run(topic: String, stockTicker: String): Unit = {
    val timestamp: Long = System.currentTimeMillis
    val stockUrl: String = s"https://api.stocktwits.com/api/2/streams/symbol/$stockTicker.json"
    val stockId: String = s"$stockTicker-$timestamp"

    val response: HttpResponse[String] = Http(stockUrl)
      .header("Content-Type", "application/json")
      .header("Charset", "UTF-8")
      .option(HttpOptions.readTimeout(10000)).asString

    System.out.println(s"Publish $stockTicker stock tweets: id $stockId")

    val json: JsObject = Json.parse(response.body).as[JsObject]
    val jsonTransformer = (__).json.update(
      __.read[JsObject].map(jsObject => jsObject ++ Json.obj("id" -> stockId))
    )

    val newJson = json.transform(jsonTransformer) match {
      case JsSuccess(jsObject, _) => jsObject
      case _ => json
    }
    System.out.println(newJson)

    producer.send(new ProducerRecord[String, String](topic, stockId, newJson.toString))
    System.out.println("Message sent successfully")
    producer.close()
  }

}

object StockTwitsProducer extends App {
  if (args.length == 2) {
    val app = new StockTwitsProducer()
    app.run(args(0), args(1))
  } else {
    System.out.println("Enter topic name and stock ticker")
  }
}
