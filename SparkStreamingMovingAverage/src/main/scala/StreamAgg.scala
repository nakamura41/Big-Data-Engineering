import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.json4s._
import org.json4s.native.JsonMethods._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.streaming._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_unixtime
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, SparkSession}

object StreamAgg {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    println("Program started.")

    val conf = new SparkConf().setMaster("local[4]").setAppName("kafkar")

    val ssc = new StreamingContext(conf,Seconds(2))



    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("stockquotes")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    //stream.map(record => parse(record.value)).print()

    stream.foreachRDD(rdd =>
      if (!rdd.isEmpty()) {
        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        val topicValueStrings = rdd.map(record => (record.value()).toString)
        val df = sqlContext.read.json(topicValueStrings)
        val movAvg = df.withColumn("movingAverage", avg(df("marketAverage"))
          .over(Window.partitionBy("date").rowsBetween(0,-4)) )

        movAvg.select("movingAverage").show()
      })
    ssc.start()
    ssc.awaitTermination()


  }
}
