
import java.nio.ByteBuffer
import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.udf

import com.datastax.driver.core.ConsistencyLevel
import com.datastax.spark.connector.writer._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.sql.SaveMode

object spid_part1 {

  def touuid(a: Array[Byte]): String = {
    val bb = ByteBuffer.wrap(a)
    val high = bb.getLong();
    val low = bb.getLong();
    val uuid = new UUID(high, low);
    return uuid.toString()
  }

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
      .config("spark.sql.broadcastTimeout", "3000")
      .getOrCreate()

    //Hbase Connection Set Up
    val jdbcConnString = "jdbc:phoenix:10.28.44.10,10.28.44.11,10.28.44.12,10.28.44.100,10.28.44.101:2181:/hbase-unsecure"
   // val jdbcConnString = "jdbc:phoenix:asthad010.wvrgroup.internal,asthad011.wvrgroup.internal,asthad012.wvrgroup.internal,asthad100.wvrgroup.internal,asthad101.wvrgroup.internal:/hbase-unsecure"
    val username = "emrhbasestage"
    val password = "GUALRpHuPT1R21qQ9k8G"
    val Driver = "org.apache.phoenix.jdbc.PhoenixDriver"
    val table = "EDAP.SCRATCHPAD"
    val table1 = "CUSTOMER_TEST.KEYCHAIN_GRAPH"

    val spdf = spark.read.format("jdbc")
      .option("url", jdbcConnString)
      .option("dbtable", table). option("user", username)
  .option("password",password ).option("fetchsize", fetch).option("driver", Driver).load()

    val spdf1 = spdf.repartition(num.toInt)
    spdf1.cache()
    spdf1.createOrReplaceTempView("tmp")

    //Transformations using Spark sql
    val spdf2 = spark.sql("select distinct(HAV_ID) as hav_id from tmp where limit 2800000")

    spdf2.createOrReplaceTempView("hav")

    import spark.implicits._

    val puuid = "00000000-0000-0000-0000-000000000001"
    val introseen = "false"
    val spdf4 = spdf2.withColumn("user_public_uuid", lit(puuid: String))

    spdf4.createOrReplaceTempView("tmp1")

    val kgdf = spark.read.format("jdbc")
      .option("url", jdbcConnString)
      .option("dbtable", table1).option("fetchsize", fetch).option("driver", Driver).load()

    kgdf.createOrReplaceTempView("kgtab")

    val kgdf1 = spark.sql("select KEY1 as hav_id1,KEY2 as user_public_uuid1 from kgtab where KEY1_TYPE_ID = 1 and KEY2_TYPE_ID = 2 limit 1400000")

    val havid = udf((x: Array[Byte]) => touuid(x))
    val kgdf2 = kgdf1.withColumn("hav_id", havid(kgdf1.col("hav_id1")))
    val kgdf3 = kgdf2.withColumn("user_public_uuid", havid(kgdf2.col("user_public_uuid1")))

    kgdf3.createOrReplaceTempView("spid1")

    val kgdf4 = spark.sql("select hav_id, user_public_uuid from spid1")
    kgdf4.createOrReplaceTempView("spid")

    // kgdf4.show
    //println(kgdf4.schema)

    val part1 = spark.sql("select tmp1.hav_id as hav_id,tmp1.user_public_uuid as user_public_uuid from tmp1 left join spid on tmp1.hav_id=spid.hav_id where spid.user_public_uuid is NULL")
    val part2 = spark.sql("select tmp1.hav_id as hav_id,spid.user_public_uuid as user_public_uuid from tmp1 inner join spid on tmp1.hav_id=spid.hav_id ")
    val part3 = spark.sql("select spid.hav_id as hav_id,spid.user_public_uuid as user_public_uuid from tmp1 right join spid on tmp1.hav_id=spid.hav_id where tmp1.user_public_uuid is NULL")

    //part3.show()

    val uni1 = part1.union(part2).union(part3)

    val uni2 = uni1.withColumn("spid", $"hav_id") //if hav_id = spid

    // println(uni2.count())

    uni2.createOrReplaceTempView("tmp4")

    val uni = spark.sql("select * from tmp4 where hav_id is not null and user_public_uuid is not null and spid is not null")

    uni.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "spid3", "keyspace" -> "scratchpad")).mode(SaveMode.Append)
      .save()
      val sc=spark.sparkContext
     // val cas= sc.cassandraTable("scratchpad", "spid3")
      //println(cas.count())
  }
}