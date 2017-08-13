
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{ColumnName, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SparkTest {

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

        val FinancesDF=spark.read.json("/Users/sriharitummala/Downloads/finances-small.json")

        FinancesDF
          .na.drop("all",Seq("ID","Account","Amount","Description","Date")) // drop empty records
          .na.fill("Unknown",Seq("Description")) // empty column in description column fill with unknown
          .where($"Amount" =!= 0 || $"Description" === "Unknown")
          .selectExpr("Account.Number as AccountNumber","Amount","Date","Description")
          .write.mode(SaveMode.Overwrite).parquet("/Users/sriharitummala/Downloads/finances-small")

        if(FinancesDF.hasColumn("_corrupt_record")){
            FinancesDF.where($"_corrupt_record".isNotNull).select($"_corrupt_record")
                        .write.mode(SaveMode.Overwrite).text("/Users/sriharitummala/Downloads/corrupt-finances")
        }

        FinancesDF
             .select(concat($"Account.FirstName",lit(" "),$"Account.LastName").as("FullName"),
                            $"Account.Number".as("AccountNumber"))
             .distinct
             .coalesce(5)
             .write.mode(SaveMode.Overwrite).json("/Users/sriharitummala/Downloads/finances-small-accounts")


        FinancesDF
               .select($"Account.Number".as("AccountNumber"),$"Amount",$"Description",$"Date")
               .groupBy($"AccountNumber")
               .agg(avg($"Amount").as("AverageTransaction"),sum($"Amount").as("TotalTransactions"),
                     count($"Amount").as("NumberOfTransactions"),max($"Amount").as("MaxTransaction"),
                     min($"Amount").as("MinTransaction"),stddev($"Amount").as("StandardDeviationAmount")
                     ,collect_set($"Description").as("UniqueTransactionDescriptions"))
               .coalesce(5)
               .write.mode(SaveMode.Overwrite).parquet("/Users/sriharitummala/Downloads/finances-small-account-details")

        spark.read.parquet("/Users/sriharitummala/Downloads/finances-small").show(20)

        spark.read.parquet("/Users/sriharitummala/Downloads/finances-small-account-details").show(truncate = false)

        val financeDetails=spark.read.parquet("/Users/sriharitummala/Downloads/finances-small-account-details")

        financeDetails
          .select($"AccountNumber",$"UniqueTransactionDescriptions",array_contains($"UniqueTransactionDescriptions","Movies").as("WentToMovies"))
          .show(truncate = false)

        financeDetails
          .select($"AccountNumber",$"UniqueTransactionDescriptions",array_contains($"UniqueTransactionDescriptions","Movies").as("WentToMovies"))
          .where(!$"WentToMovies")
          .show(truncate = false)

        financeDetails
          .select($"AccountNumber",size($"UniqueTransactionDescriptions").as("CountOfUniqueTransactionTypes"))
          .show(truncate = false)

        financeDetails
          .select($"AccountNumber",size($"UniqueTransactionDescriptions").as("CountOfUniqueTransactionTypes")
          ,sort_array($"UniqueTransactionDescriptions",asc=false).as("OrderedUniqueTransactionDescriptions"))
          .show(truncate = false)

        //println(spark.read.parquet("/Users/sriharitummala/Downloads/finances-small").count())
    }

        implicit class DataFrameHelper(df:DataFrame){
            import scala.util.Try
            def hasColumn(columnName: String)=Try(df(columnName)).isSuccess
        }




}
