package com.bigdata

import java.util.concurrent._
import java.util.{Collections, Properties}

import kafka.utils.Logging
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.collection.JavaConversions._

class SimpleConsumer(val topic: String) extends Logging {

  val props: Properties = createConsumerConfig()
  val consumer = new KafkaConsumer[String, String](props)
  var executor: ExecutorService = _

  def shutdown(): Unit = {
    if (consumer != null)
      consumer.close()
    if (executor != null)
      executor.shutdown()
  }

  def createConsumerConfig(): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }

  def run(): Unit = {
    consumer.subscribe(Collections.singletonList(this.topic))
    Executors.newSingleThreadExecutor.execute(() => {
      while (true) {
        val records = consumer.poll(1000)
        for (record <- records.iterator()) {
          System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
        }
      }
    })
  }

}

object SimpleConsumer extends App {
  if (args.length == 0) {
    System.out.println("Enter topic name")
  } else {
    val app = new SimpleConsumer(args(0))
    app.run()
  }
}
