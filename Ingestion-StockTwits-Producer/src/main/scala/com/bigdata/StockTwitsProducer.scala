package com.bigdata

import java.util.Properties
import java.util.HashMap

import kafka.utils.Logging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import scalaj.http._
import com.redis._
import org.json4s._
import org.json4s.native.JsonMethods._
import com.google.gson.GsonBuilder


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
      case JString(i) => i.toString
    }
  }

  def getLatestStockId(stockTicker: String, redisHost: String, database: Integer): String = {
    val redis = new RedisClient(redisHost, port = 6379, database = database)
    val value: Option[String] = redis.get(stockTicker)
    value match {
      case Some(i) => i
      case None => "0"
    }
  }

  def setLatestStockId(id: String, stockTicker: String, redisHost: String, database: Integer): Unit = {
    val redis = new RedisClient(redisHost, port = 6379, database = database)
    redis.set(stockTicker, id)
  }

  def run(topic: String, stockTicker: String, redisHost: String): Unit = {
    val REDIS_DATABASE = 1

    val timestamp: Long = System.currentTimeMillis

    val latestStockId = getLatestStockId(stockTicker, redisHost, REDIS_DATABASE)
    var stockUrl: String = s"https://api.stocktwits.com/api/2/streams/symbol/$stockTicker.json"
    if (latestStockId != "0") {
      stockUrl = stockUrl + s"?since=$latestStockId"
    }
    System.out.println(s"Pull stock tweets from $stockUrl")

    val response: HttpResponse[String] = Http(stockUrl)
      .header("Content-Type", "application/json")
      .header("Charset", "UTF-8")
      .option(HttpOptions.readTimeout(10000)).asString

    val json = parse(response.body)
    val jsonResponse = (json \ "response" \ "status").values
    if (jsonResponse == 200) {
      val jsonCursor = json \ "cursor"
      val jsonSince: Int = getInteger(jsonCursor \ "since")
      val jsonMax: Int = getInteger(jsonCursor \ "max")
      val since: BigInt = if (jsonSince > jsonMax) jsonMax else jsonSince
      val max: BigInt = if (jsonSince > jsonMax) jsonSince else jsonMax

      System.out.println(s"since ID: $since, max ID: $max")

      val messages: List[JsonAST.JValue] = (json \ "messages").children
      for (message <- messages) {
        val JInt(messageId) = message \ "id"
        val stockId: String = s"$stockTicker-$messageId"

        val messsageMap = message

        val jsonMap: HashMap[String, Any] = new HashMap[String, Any]()

        val jsonId: Int = getInteger(message \ "id")
        val jsonBody: String = getString(message \ "body")
        val jsonUserFollowers: Int = getInteger(message \ "user" \ "followers")
        val jsonUserUsername: String = getString(message \ "user" \ "username")
        val jsonUserName: String = getString(message \ "user" \ "name")
        val jsonLikesTotal: Int = getInteger(message \ "likes" \ "total")
        val jsonEntitiesSentimentBasic: String = getString(message \ "entities" \ "sentiment" \ "basic")

        jsonMap.put("symbol", stockTicker)
        jsonMap.put("created_at", System.currentTimeMillis)
        jsonMap.put("id", jsonId)
        jsonMap.put("body", jsonBody)
        jsonMap.put("user_followers", jsonUserFollowers)
        jsonMap.put("user_username", jsonUserUsername)
        jsonMap.put("user_name", jsonUserName)
        jsonMap.put("likes_total", jsonLikesTotal)
        jsonMap.put("entities_sentiment_basic", jsonEntitiesSentimentBasic)

        val gsonMapBuilder = new GsonBuilder
        val gsonObject = gsonMapBuilder.create
        val jsonObject = gsonObject.toJson(jsonMap)

        producer.send(new ProducerRecord[String, String](topic, stockId, jsonObject.toString))
        System.out.println(s"Publish $stockTicker stock tweets: id $stockId")
      }
      System.out.println("Message sent successfully")

      // update latest redis
      if (max > latestStockId.toInt) {
        setLatestStockId(max.toString, stockTicker, redisHost, REDIS_DATABASE)
      }

    } else {
      logger.error("Failed to get response from StockTwits API")
    }
    producer.close()
  }

}

object StockTwitsProducer extends App {
  if (args.length == 3) {
    val app = new StockTwitsProducer()
    app.run(args(0), args(1), args(2))
  } else {
    System.out.println("Enter topic name, stock ticker, and redis host")
  }
}
