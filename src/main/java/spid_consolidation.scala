import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number

import com.datastax.driver.core.ConsistencyLevel
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.driver.core.Session

case class test1(hav_id: String,
                 spid: String,
                 user_public_uuid: String)

object spid_consolidation {
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

    val spid_df1 = spark.read.
      format("org.apache.spark.sql.cassandra").
      options(Map("table" -> "spid3", "keyspace" -> "scratchpad")).load
      
      spid_df1.createOrReplaceTempView("puuid")
     
      spid_df1.cache()
      
     val spid_df = spark.sql("select * from puuid where user_public_uuid != '00000000-0000-0000-0000-000000000001'") 
      
    val w = Window.partitionBy(spid_df.col("user_public_uuid")).orderBy(spid_df.col("spid"))

    val tmpdf = spid_df.withColumn("rn", row_number.over(w))

    tmpdf.createOrReplaceTempView("tmp")

    val tmpdf1 = spark.sql("select user_public_uuid,spid from tmp where rn=1") //.where("$rn" === 1).select("user_public_uuid", "spid")

    val tmpdf2 = tmpdf1.join(spid_df.select("user_public_uuid", "hav_id"), Seq("user_public_uuid"), "inner")

    //tmpdf2.map(x => test.apply(x(2).asInstanceOf[String], x(1).asInstanceOf[String], x(0).asInstanceOf[String])) 
    
    //val df3= spid_df.unionAll(tmpdf2)
 
    val tmpdf3= spark.sql("select * from puuid where user_public_uuid = '00000000-0000-0000-0000-000000000001'")
    
    val tmpdf4 = tmpdf3.union(tmpdf2)
    
    tmpdf4.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "spid4", "keyspace" -> "scratchpad")).mode(SaveMode.Append)
      .save()

    /*val sc = spark.sparkContext
    val castab = sc.cassandraTable("scratchpad", "spid1")
    castab.cache()
    val c = CassandraConnector(sc.getConf)

    //castab.foreach(row => c.withSessionDo(session => session.execute(s"DELETE FROM scratchpad.spid1 WHERE hav_id ='"+row._1+"';")))

    castab.foreachPartition(partition => {
    val session: Session = c.openSession //once per partition
    partition.foreach{elem => 
        val delete = "DELETE FROM scratchpad.spid1 where hav_id = '"+elem+"';"
        session.execute(delete)
    }
    session.close()
})*/
  }
}