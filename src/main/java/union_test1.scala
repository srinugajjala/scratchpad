import org.apache.spark.sql.SparkSession
import com.datastax.driver.core.ConsistencyLevel
import org.apache.spark.sql.SaveMode


object union_test1 {
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
    
      val spid_df = spark.read.
      format("org.apache.spark.sql.cassandra").
      options(Map("table" -> "spid", "keyspace" -> "scratchpad")).load
      
      val spid1_df = spark.read.
      format("org.apache.spark.sql.cassandra").
      options(Map("table" -> "spid1", "keyspace" -> "scratchpad")).load
      
      val df3= spid1_df.union(spid_df)
      
    //  df3.show(40)
    // println(df3.count())
      
   df3.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "spid1", "keyspace" -> "scratchpad")).mode(SaveMode.Append)
      .save()
  }
}