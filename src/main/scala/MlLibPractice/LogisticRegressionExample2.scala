package MlLibPractice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}

object LogisticRegressionExample2 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark =SparkSession.builder
      .master("local[*]")
      .appName("Fraud Detector")
      .config("spark.driver.memory","2g")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    /*height in inches,weight*/

    val lebron = (1.0,Vectors.dense(80.0,250.0))
    val tim = (0.0,Vectors.dense(70.0,150.0))
    val brittany = (1.0,Vectors.dense(80.0,207.0))
    val stacey = (0.0,Vectors.dense(65.0,120.0))

    val training = spark.createDataFrame(Seq(lebron,tim,brittany,stacey)).toDF("label","features")
    val estimator = new LogisticRegression

    val transformer = estimator.fit(training)
    val john = Vectors.dense(90.0,270.0)
    val tom = Vectors.dense(62.0,120.0)

    val test = spark.createDataFrame(Seq((1.0, john),(0.0, tom))).toDF("label", "features")

    val results = transformer.transform(test)
    results.printSchema

    results.show

    //val predictions = results.select          ("features","prediction").show



  }

}
