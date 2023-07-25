import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import SparkRDD_DF._


import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra._
object Spark_Cassandra {

  def main(args: Array[String]): Unit = {

    val ss = Session_Spark(true)

    ss.conf.set(s"ss.sql.catalog.ibh","com.datastax.spark.connector.datasource.CassandraCatalog") // define catalog
    ss.conf.set(s"ss.sql.catalog.ibh.spark.cassandra.host","localhost") // ip address

    ss.sparkContext.cassandraTable ("demo","spacecraft_journey_catalog")

  val df_cassandra = ss.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "demo" , "table" -> "spacecraft_journey_catalog", "cluster"-> "journey_id"))
      .load()

    df_cassandra.printSchema()
    df_cassandra.explain()
    df_cassandra.show(5)

    val df_cassandra2 = ss.read
      .cassandraFormat("spacecraft_journey_catalog", "demo", "journey_id")
      .load()

    df_cassandra2.show(5)






  }
}
