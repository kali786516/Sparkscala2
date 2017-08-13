import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{ColumnName, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.scalalang.typed

case class Account(number:String,firstName:String,lastName:String)
case class Transaction(id:Long,account:Account,date:java.sql.Date,amount:Double,description:String)
case class TransactionForAverage(accountNumber:String,amount:Double,description:String,date:java.sql.Date)

object DataSets {

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

    val FinancesDS=spark.read.json("/Users/sriharitummala/Downloads/finances-small.json")
                     .withColumn("date",to_date(unix_timestamp($"Date","MM/dd/yyyy").cast("timestamp"))).as[Transaction]

    val accountNumberPrevious4WindowSpec=Window
                                         .partitionBy($"AccountNumber")
                                         .orderBy($"Date")
                                         .rowsBetween(-4,0)

    val rollingAvgForPrevious4PerAccount=avg($"Amount").over(accountNumberPrevious4WindowSpec)

    FinancesDS
                .na.drop("all",Seq("ID","Account","Amount","Description","Date"))
                  .na.fill("Unknown",Seq("Description")).as[Transaction]
                    .filter(tx=> (tx.amount!=0 || tx.description == "Unknown"))
                      .select($"Account.Number".as("AccountNumber").as[String],$"Amount".as[Double],
                      $"Date".as[java.sql.Date](Encoders.DATE),$"Description".as[String])
                        .withColumn("RollingAverage",rollingAvgForPrevious4PerAccount)
                          .write.mode(SaveMode.Overwrite).parquet("/Users/sriharitummala/Downloads/finances-small")

    if(FinancesDS.hasColumn("_corrupt_record")){
      FinancesDS.where($"_corrupt_record".isNotNull).select($"_corrupt_record")
        .write.mode(SaveMode.Overwrite).text("/Users/sriharitummala/Downloads/corrupt-finances")
    }


    FinancesDS
                .map(tx => (s"${tx.account.firstName} ${tx.account.lastName}",tx.account.number))
                  .distinct()
                    .toDF("FullName","AccountNumber")
                      .coalesce(5)
                        .write.mode(SaveMode.Overwrite).json("/Users/sriharitummala/Downloads/finances-small-accounts")

    FinancesDS
                .select($"account.number".as("accountNumber").as[String],$"amount".as[Double],
                $"descritption".as[String],
                $"date".as[java.sql.Date](Encoders.DATE)).as[TransactionForAverage]
                      .groupByKey(_.accountNumber)
                        .agg(
                          typed.avg[TransactionForAverage](_.amount).as("AverageTransaction").as[Double],
                          typed.sum[TransactionForAverage](_.amount),
                          typed.count[TransactionForAverage](_.amount),
                          max($"Amount").as("MaxTransaction").as[Double]
                        )
                         .coalesce(5)
                           .write.mode(SaveMode.Overwrite).json("/Users/sriharitummala/Downloads/finances-small-account-details")

  }

  implicit class DatasetHelper[T](ds:Dataset[T]){
    import scala.util.Try
    def hasColumn(columnName: String)=Try(ds
    (columnName)).isSuccess
  }




}
