import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{BooleanType, FloatType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

class BatchPredictApp() {

  def run(host: String, port: String): Unit = {

    print("Host: " + host)
    print("Port: " + port)
    val conf = new SparkConf()
    conf.setMaster("local") // run locally, rather than in distributed mode
    conf.setAppName("BatchPredictApp")

//    val sparkSession = SparkSession.builder.appName("BatchPredict").
//      config("spark.cassandra.connection.host", host).
//      config("spark.cassandra.connection.port", port).master("local").getOrCreate()

    val sparkSession = SparkSession.builder.appName("BatchPredictApp").
      config("spark.cassandra.connection.host", host).
      config("spark.cassandra.connection.port", port).master("local").getOrCreate()

    import sparkSession.implicits._
    val df = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "stock_quote_batch", "keyspace" -> "bigdata"))
      .load()

    df.printSchema()

    // Preprocessing
    val filterDF = df
      .filter($"symbol" === "AAPL")
      .sort($"created_at".asc)

    filterDF.show()
    print("Number of rows: " + filterDF.count())

    val work_df = filterDF.select("created_at", "marketaverage")

    val featureCols = Array("created_at")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val data = assembler.transform(work_df)
    val dropCols = Seq("created_at")
    val dfDropCols = data.drop(dropCols: _*)
    dfDropCols.show()


    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = dfDropCols.randomSplit(Array(0.7, 0.3))

    // Train a RandomForest model.
    val rf = new RandomForestRegressor()
      .setLabelCol("marketaverage")
      .setFeaturesCol("features")

    val model = rf.fit(trainingData)
    // Make predictions.
    var predictions = model.transform(testData)

    //    // Select example rows to display.
    predictions.select("prediction", "marketaverage", "features").show(5)
    //
    // Select (prediction, true label) and compute test error.
    val evaluator = new RegressionEvaluator()
      .setLabelCol("marketaverage")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse")
    //
    val rfModel = model.asInstanceOf[RandomForestRegressionModel]
    println(s"Learned regression forest model:\n ${rfModel.toDebugString}")


    import org.apache.spark.sql.functions._
    import org.apache.spark.ml._
    // A UDF to convert VectorUDT to ArrayType
    val vecToArray = udf((xs: linalg.Vector) => xs.toArray(0))

    // Add a ArrayType Column
    predictions = predictions.withColumn("epoch", vecToArray($"features"))
    predictions.printSchema()


    import org.apache.spark.sql.functions.from_unixtime

    print("Finished running batch predictions.")
    predictions = predictions.withColumn("symbol", lit("AAPL"))
      .withColumn("id", monotonically_increasing_id())
      .withColumn("job_at", lit(System.currentTimeMillis / 1000))
      .withColumn("date", lit(from_unixtime($"epoch", "yyyy-MM-dd")))
      .withColumn("hour", lit(from_unixtime($"epoch", "HH")))
      .select("symbol", "id", "job_at", "epoch", "date", "hour", "marketaverage", "prediction")


    predictions.show()
    predictions.printSchema()
    print("Number of APPL rows: " + filterDF.count())
    print("Number of prediction rows: " + predictions.count())

    predictions
      .write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "bigdata", "table" -> "batch_predict_results"))
      .mode(SaveMode.Append)
      .save()

    sparkSession.stop()

  }
}

object BatchPredictApp extends App {
  if (args.length == 2) {
    val app = new BatchPredictApp()
    app.run(args(0), args(1))
  } else {
    System.out.println("Enter host and port as arguments.")
  }
}