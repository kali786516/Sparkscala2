package MlLibPractice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

object CorelationExample {

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

    val df=spark.createDataFrame(List(("Kali",121),("uma",120)))
    val houses = spark.createDataFrame(Seq((1620000d,2100),
      (1690000d,2300),(1400000d,2046),(2000000d,4314),(1060000d,1244),(3830000d,4608),(1230000d,2173),(2400000d,2750),(3380000d,4010),
      (1480000d,1959))).toDF("price","size")

    //calculate corelation
    println(houses.stat.corr("price","size"))




  }

}
