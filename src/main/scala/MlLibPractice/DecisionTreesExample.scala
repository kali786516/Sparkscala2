package MlLibPractice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.Entropy
import org.apache.spark.sql.SparkSession

object DecisionTreesExample {

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

    val data = spark.sparkContext.textFile("tennis.csv")

    val parseddata=data.map{
      line => val parts = line.split(",").map(_.toDouble)
        LabeledPoint(parts(0),Vectors.dense(parts.tail))
    }

    val model = DecisionTree.train(parseddata, Classification, Entropy, 3)

    val v=Vectors.dense(0.0,1.0,0.0)
    model.predict(v)







  }

}
