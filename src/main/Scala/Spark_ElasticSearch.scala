import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import SparkRDD_DF._


import org.elasticsearch.spark.sql._

object Spark_ElasticSearch {

  def main(args : Array [String]) : Unit ={

    val ss =Session_Spark(true)

    val df_orders = ss.read
    .format("com.databricks.spark.csv")
    .option("delimiter", ";")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\Doc\\Data\\orders.csv")

   // df_orders.show(5)

    // create index from DF ; create index if does not exists
    df_orders.write
      .format("org.elasticsearch.spark.sql")
      .mode(SaveMode.Append)
      .option("es.port","9200")
      .option("es.nodes","localhost")
      .save("index_ibh/doc")

  // method  2 by using all settings in config
    val session_s = SparkSession.builder()
      .appName("My Spark Application")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("es.port", "9200")
      .config("es.nodes", "localhost")
      .enableHiveSupport()

    df_orders.saveToEs("index_ibh/doc")



  }
}
