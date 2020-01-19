package MlLibPractice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DoubleType, StructType}
// In Spark 1x.
// import org.apache.spark.mllib.linalg.{Vectors, VectorUDT}
//import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType

object LogesticRegressionSmall {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)



    import org.apache.spark.sql.SQLImplicits
    //import spark.implicits._
    import sqlContext.implicits._

    /*val df=spark.createDataFrame(List(("Kali",121),("uma",120)))
    val namesDF=df.toDF("Username","Score")
    namesDF.count()*/

    /*real data*/

    val lebron = LabeledPoint (1.0,Vectors.dense (80.0,250.0))
    val tim = LabeledPoint (0.0, Vectors.dense (70.0,150.0))
    val brittany = LabeledPoint (1.0,Vectors.dense (80.0,207.0))
    val  stacey = LabeledPoint (0.0,Vectors.dense (65.0,120.0))
    val trainingRDD = sc.parallelize(List(lebron,tim,brittany,stacey))
    val trainingDF = trainingRDD.toDF

    val estimator = new LogisticRegression
    val transformer = estimator.fit (trainingDF)

    /*test data*/

    val john = Vectors.dense (90.0,27-0.0)
    val tom = Vectors.dense (62.0,120.0)
    val testRDD = sc.parallelize(List ( john, tom))



    case class Feature (v:Vector)
    val featuresRDD = testRDD.map( v => Feature (v) )
    //val featuresDF = featuresRDD.toDF("features")

    /*predict join which is called transform in scala spark */
    //val predictionsDF = transformer.transform (featuresDF)
    //predictionsDF.show(10,false)

    //predictionsDF.printSchema()


    //val shorterpredictionsDF = predictionsDF.select ("features", "prediction")

    /*here we are renaming a column name prediction to isbasketballplayer*/

    //val playerDF = shorterpredictionsDF.toDF ("features", "isbasketballplayer")
    //playerDF.printSchema()







  }

}
