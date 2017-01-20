import org.apache.spark.sql.SparkSession

import com.datastax.spark.connector._
import com.datastax.driver.core.ConsistencyLevel

import java.util.UUID

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.{ lit, udf }
import org.apache.spark.sql.SaveMode

import com.databricks.spark.avro._
import java.util.UUID
object SPID {
  def main(args: Array[String]): Unit = {
    val oprows = args(0)
    val batch = args(1)
    val cwrites = args(2)
    val fetch = args(3)
    val chost = args(4)
    val user = args(5)
    val pass = args(6)
    val no_of_partitions = args(7)
    val upperlimit = args(8)
    val num = args(9)
    val filename = args(10)

    //1 5 72000 72000 10.123.2.225 cassandra cassandra 23 20000000 13 twentymi9  
    //val user = "srao"
    //val pas= """'$R@0DevAccess'"""

    val conLevel = ConsistencyLevel.LOCAL_ONE.toString()

    val spark = SparkSession.builder.master("local")
      .appName("spark session example")
      .config("spark.cassandra.connection.host", chost)
      .config("spark.cassandra.auth.username", user)
      .config("spark.cassandra.auth.password", pass)
      .config("spark.cassandra.output.batch.size.rows", oprows)
      .config("spark.cassandra.output.batch.grouping.buffer.size", batch)
      .config("spark.cassandra.output.concurrent.writes", cwrites)
      .config("spark.output.consistency.level", conLevel)
      .config("hbase.rpc.timeout", "1800000")
      .getOrCreate()

    //Hbase Connection Set Up
    val jdbcConnString = "jdbc:phoenix:10.28.44.10,10.28.44.11,10.28.44.12,10.28.44.100,10.28.44.101:/hbase-unsecure"
    val username = "unsecure"
    val password = "unsecure"
    val Driver = "org.apache.phoenix.jdbc.PhoenixDriver"
    val table = "EDAP.SCRATCHPAD"
    
    val df = spark.read.format("jdbc")
      .option("url", jdbcConnString)
      .option("dbtable", table).option("fetchsize", fetch).option("driver", Driver).load()

    val df1 = df.repartition(num.toInt)
    df1.cache()
    df1.createOrReplaceTempView("tmp")

    //Transformations using Spark sql
    val df2 = spark.sql("select distinct(HAV_ID) as hav_id from tmp limit 40")
    
    df2.createOrReplaceTempView("hav")
    
   // val spid= df2.withColumnRenamed("hav_id", "spid")
    //spid.createOrReplaceTempView("spid")
    
   // val df5 = spark.sql("select hav.hav_id, spid.spid from hav join spid on hav.hav_id=spid.spid")
      
    import spark.implicits._
  //  val generateUUID = udf(() => UUID.randomUUID().toString)
//    val df3 = df2.withColumn("spid", generateUUID()) // generate spid
    
    val df3 = df2.withColumn("spid", $"hav_id") //if hav_id = spid

    val puuid = "00000000-0000-0000-0000-000000000002"
    val introseen = "false"
    val df4 = df3.withColumn("user_public_uuid", lit(puuid: String))

    //For testing if you want to see data frame subset
    //df4.show(10)

    df4.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "spid", "keyspace" -> "scratchpad")).mode(SaveMode.Append)
      .save()

    df4.createOrReplaceTempView("tmp1")

  }
}