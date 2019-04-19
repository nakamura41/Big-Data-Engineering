import org.apache.spark.sql.SparkSession


object CassandraReader{
  def main(args: Array[String]): Unit ={


    val sparkSession = SparkSession.builder.appName("CassandraReader").
      config("spark.cassandra.connection.host", "18.136.251.110").
      config("spark.cassandra.connection.port", "9042").master("local[1]").getOrCreate()

    import sparkSession.implicits._

    val df = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "stock_quote_batch", "keyspace" -> "bigdata" ))
      .load()

    //print(df.show(20, false))
    df.collect.foreach(println)
    df.printSchema()
  }
}