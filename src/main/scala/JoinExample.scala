import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{ColumnName, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.scalalang.typed

case class Person(id:Int,first:String,last:String)

case class Role(id:Int,role:String)

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
