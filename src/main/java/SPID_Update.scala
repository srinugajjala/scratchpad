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

 /* private UUID fromBytes(byte[] bytes) {
       if (bytes == null) {
           return null;
       } else {
           ByteBuffer bb = ByteBuffer.wrap(bytes);
           long high = bb.getLong();
           long low = bb.getLong();
           return new UUID(high, low);
       }
   }*/

case class test(hav_id:String,
                spid:String,
                user_public_uuid:String)

object SPID_Update {
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
    val table = "CUSTOMER_TEST.KEYCHAIN_GRAPH"

    val touuid = udf((x: Array[Byte]) => UUID.nameUUIDFromBytes(x).toString())
    val buff = udf((x: Array[Byte]) => ByteBuffer.wrap(x));
    val touuid1= udf((x: Array[Byte]) => new UUID(ByteBuffer.wrap(x).getLong(), ByteBuffer.wrap(x).getLong()))
    
  //  val toString= udf((x: String) => ByteBuffer.wrap(x))
   /* val spid_df = spark.read.
      format("org.apache.spark.sql.cassandra").
      options(Map("table" -> "spid", "keyspace" -> "scratchpad")).load

    spid_df.createOrReplaceTempView("spid")*/
    
    val sc= spark.sparkContext
    val castab= sc.cassandraTable("scratchpad", "spid")
    
    
   // spid_df.show()

    val df = spark.read.format("jdbc")
      .option("url", jdbcConnString)
      .option("dbtable", table).option("fetchsize", fetch).option("driver", Driver).load()

    df.createOrReplaceTempView("tmp")
    
   // val del_rows= spark.sql("select spid_df.spid,spid_df.hav_id,spid_df.user_public_uuid from spid_df join tmp on spid_df.hav_id=tmp.hav_id")
    
    import spark.implicits._
     //val rdd= del_rows.map(x => test.apply(x(0).asInstanceOf[String], x(1).asInstanceOf[String], x(2).asInstanceOf[String])).rdd
    
     //val config= CassandraConnectorConf
    
    //val cc= CassandraConnector(config)
     
     import spark.implicits._   
   castab.map( row => castab.connector.withSessionDo( session => session.execute("DELETE FROM scratchpad.spid WHERE hav_id = ?", "row.hav_id")))
   
    //del_rows.deleteFromCassandra("","")
    
   // val dftest= spark.sql("select * from tmp where KEY1 = toString('45295744916E470EA75096ECBD1B1487', 'HEX');")
    //dftest.show()
    
    val df2 = spark.sql("select KEY1 as hav_id,KEY2 as user_public_uuid,UPDATED from tmp where KEY1_TYPE_ID = 1 and KEY2_TYPE_ID = 2 and UPDATED= 1452450727146 order by KEY1, KEY2_TYPE_ID, KEY2 limit 10")
   // val df2= spark.sql("select * from tmp where KEY2_TYPE_ID = 2 and KEY1_TYPE_ID = 1 limit 10;")//order by key1, key2_type_id, key2
    df2.show()

    // val touuid= UUID.nameUUIDFromBytes(x$1)
    
    // val touuid = udf((x:Array[Byte]) => UUID.nameUUIDFromBytes(x).toString)

    val df6 = df2.withColumn("spid1", touuid1(df2.col("hav_id")))
    val df7 = df6.withColumn("puuid1", touuid1(df2.col("user_public_uuid")))

    df7.show()

    //DECODE(KEY1, 'HEX')
    /*   
    
    df2.createOrReplaceTempView("tmp1")
    
    val df3= spark.sql("select tmp1.hav_id as hav_id,tmp1.user_public_uuid as user_public_uuid,spid.spid as spid from tmp1 join spid on tmp1.hav_id=spid.hav_id where user_public_uuid = '00000000-0000-0000-0000-000000000002'")
    df3.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "spid", "keyspace" -> "scratchpad")).mode(SaveMode.Append)
      .save() */
  }
  
  
  
}