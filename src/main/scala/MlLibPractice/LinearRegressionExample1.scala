package MlLibPractice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression

object LinearRegressionExample1 {

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

    /*housr price and sqt foot*/

    val points = spark.createDataFrame(Seq((1620000,Vectors.dense(2100)),(1690000,Vectors.dense(2300)),
      (1400000,Vectors.dense(2046)),(2000000,Vectors.dense(4314)),(1060000,Vectors.dense(1244)),
      (3830000,Vectors.dense(4608)),(1230000,Vectors.dense(2173)),(2400000,Vectors.dense(2750)),
      (3380000,Vectors.dense(4010)),(1480000,Vectors.dense(1959)))).toDF("label","features")

    val lr = new LinearRegression()
    val model = lr.fit(points)
    val test = spark.createDataFrame(Seq(Vectors.dense(2100)).map(Tuple1.apply)).toDF("features")

    val predictions = model.transform(test)


    predictions.show()








  }

}
