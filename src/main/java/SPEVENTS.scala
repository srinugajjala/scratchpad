import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object SPEVENTS {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local")
      .appName("spark session example")
      .getOrCreate()


      
  }
}