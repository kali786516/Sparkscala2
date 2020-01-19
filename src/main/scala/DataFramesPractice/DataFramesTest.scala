package DataFramesPractice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DataFramesTest {

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
    val namesDF=df.toDF("Username","Score")
    namesDF.count()

    val extendDF=namesDF.withColumn("ScorewithoutBase",$"Score" - 50)
    val baseDF=extendDF.drop("ScorewithoutBase")
    val financeDF=spark.sql("SELECT * FROM  parquet.`/Users/sriharitummala/Downloads/finances-small`")
    financeDF.createOrReplaceTempView("Finances")
    financeDF.selectExpr("AccountNumber","Amount","Date").show


  }

}
