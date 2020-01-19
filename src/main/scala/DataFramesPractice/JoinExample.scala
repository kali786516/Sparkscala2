package DataFramesPractice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object JoinExample {

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



    val personDS=List(Person(1,"Justin","Pihony"),Person(2,"John","Doe"),Person(3,"Jane","Smith")).toDS

    val roleDS=List(Role(1,"Manager"),Role(3,"CEO"),Role(4,"Huh")).toDS

    val innerJoinDS=personDS.joinWith(roleDS,personDS("id")===roleDS("id"))

    innerJoinDS.show()

    personDS.joinWith(roleDS,personDS("id")===roleDS("id"),"outer").show()



  }

}
