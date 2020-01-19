package MlLibPractice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.linalg.{Vector, Vectors}

object LogisticRegressionSpiderExample {

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

    /*house price and sqt foot*/

    /*spider present or not , sand grain size*/

    val trainingDataSet = spark.createDataFrame(Seq((0.0,Vectors.dense(0.245)),(0.0,Vectors.dense(0.247)),
      (1.0,Vectors.dense(0.285)),(1.0,Vectors.dense(0.299)),
      (1.0,Vectors.dense(0.327)),(1.0,Vectors.dense(0.347)),(0.0,Vectors.dense(0.356)),
      (1.0,Vectors.dense(0.36)),(0.0,Vectors.dense(0.363)),
      (1.0,Vectors.dense(0.364)),(0.0,Vectors.dense(0.398)),
      (1.0,Vectors.dense(0.4)),(0.0,Vectors.dense(0.409)),(1.0,Vectors.dense(0.421)),
      (0.0,Vectors.dense(0.432)),(1.0,Vectors.dense(0.473)),(1.0,Vectors.dense(0.509)),
      (1.0,Vectors.dense(0.529)),(0.0,Vectors.dense(0.561)),(0.0,Vectors.dense(0.569)),
      (1.0,Vectors.dense(0.594)),(1.0,Vectors.dense(0.638)),(1.0,Vectors.dense(0.656)),
      (1.0,Vectors.dense(0.816)),(1.0,Vectors.dense(0.853)),(1.0,Vectors.dense(0.938)),
      (1.0,Vectors.dense(1.036)),(1.0,Vectors.dense(1.045)))).toDF("label","features")

    val lr = new LogisticRegression
    val model = lr.fit(trainingDataSet)

    val trainingSummary = model.summary

    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]
    println(s"areaUnderROC: ${binarySummary.areaUnderROC}")








  }

}
