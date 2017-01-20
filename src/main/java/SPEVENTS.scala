import org.apache.spark.sql.SparkSession
import com.datastax.driver.core.ConsistencyLevel
import org.apache.spark.sql.SaveMode

object SPEVENTS {
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
    df1.createOrReplaceTempView("tmp")

    val spid_df = spark.read.
      format("org.apache.spark.sql.cassandra").
      options(Map("table" -> "spid1", "keyspace" -> "scratchpad")).load

    spid_df.createOrReplaceTempView("tmp1")
    
    val df6 = spark.sql("select tmp.HAV_ID as hav_id, CAST(tmp.SDATE as date) as event_startdate, CAST (tmp.EDATE as date) as event_enddate,tmp.EVENT_TYPE as event_type, tmp.PARENT_UUID as parent_pageview_id, tmp.CURR_PAGEVIEW_ID as pageview_id,tmp.LISTING_ID as listing_triad,tmp.KEYWORDS as keywords, tmp.REFINE_BUCKET as refine_bucket,tmp.SEARCH_TERM as search_term,tmp.CURR as currency,tmp.PAGEHREF as page_href, tmp.PAGENAME as page_name, tmp.EVENT_TS as created,tmp.MIN_SLEEPS as guests, tmp1.spid AS spid, tmp1.user_public_uuid as user_public_uuid from tmp JOIN tmp1 ON tmp.HAV_ID = tmp1.HAV_ID WHERE tmp.CURR_PAGEVIEW_ID IS NOT NULL AND tmp.LISTING_ID IS NOT NULL limit 10")
    
    df6.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "spevents", "keyspace" -> "scratchpad")).mode(SaveMode.Append)
      .save()
    
      
  }
}