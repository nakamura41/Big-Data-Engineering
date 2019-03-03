import org.apache.spark.{SparkConf, SparkContext}

object HelloScala {

  def main(args: Array[String]) {
    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local") // run locally, rather than in distributed mode
    conf.setAppName("Word Count")
    val sc = new SparkContext(conf)

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    //val textFile = sc.textFile("src/main/resources/shakespeare.txt")
    val textFile = sc.textFile("hdfs:///tmp/shakespeare.txt")

    //word count
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _) // shorthand for : reduceByKey(x,y)=> (x + y)
    // reduceByKey when you see x and y , return x+y

    counts.foreach(println)
    System.out.println("Total words: " + counts.count());
    //counts.saveAsTextFile("/tmp/shakespeareWordCount")
    counts.saveAsTextFile("hdfs:///tmp/shakespeareWordCount")
  }
}