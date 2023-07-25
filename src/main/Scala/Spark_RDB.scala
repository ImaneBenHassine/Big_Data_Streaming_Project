import SparkRDD_DF.Session_Spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


import java.util._ // for the properties of the cnx
object Spark_RDB {
  def main (args: Array[String]): Unit = {

    val ss = Session_Spark(true)
    val props_mysql = new Properties()
    props_mysql.put("user", "analystdata")
    props_mysql.put("password", "pwd#90")

    val props_postgres = new Properties()
    props_postgres.put("user", "postgres")
    props_postgres.put("password", "pwd#90")

    val props_SQLServer = new Properties()
    props_SQLServer.put("user", "analystdata")
    props_SQLServer.put("password", "pwd#90")

/*
/**********************************************************   Mysql *********************************************************/
    // adding "?zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC" to the db schema to avoid error od due date
    val df_mysql = ss.read.jdbc("jdbc:mysql://127.0.0.1:3306/spark_db?zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC","spark_db.order_bd", props_mysql )

   //df_mysql.show(10)
    df_mysql.printSchema()

    // add aggregation when reading data --> .read.format
     val df_mysql2 = ss.read
       .format("jdbc")
       .option("url","jdbc:mysql://127.0.0.1:3306/spark_db?zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC")
       .option("user", "analystdata")
       .option("password", "pwd#90")
       .option("query", "select state, city , sum(round(numunits * totalprice)) as  total_amount from spark_db.order_bd group by state, city")
       .load()
    df_mysql2.show(5)
/*
    // save df as Mysql table
    df_mysql2.write
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/spark_db")
      .option("dbtable", "orders_city")
      .option("user", "analystdata")
      .option("password", "pwd#90")
      .save()
*/
*/

    /********************************************************** Postgresql *********************************************************/
    /*

           val df_postgres = ss.read.jdbc("jdbc:postgresql://localhost:5432/Spark_db","orders", props_postgres )

            val df_postgres = ss.read
            .format("jdbc")
            .option("url", "jdbc:postgresql://127.0.0.1:5432/")
            .option("dbtable", "orders")
            .option("user", "postgres")
            .option("password", "pwd#90")
            .load()

       df_postgres.show(5)
     */
/********************************************************** SQL *********************************************************/

  //val df_sqlserver = ss.read.jdbc("jdbc:sqlserver://DESKTOP-O58PAUC\\SPARKSQLSERVER:1433;databaseName=spark_db;", "orders", props_SQLServer)
   // IntegratedSecurity = true : cause we use windows authentifecation for sqlserver
  val df_sqlserver = ss.read
    .format("jdbc")
    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("url", "jdbc:sqlserver://DESKTOP-O58PAUC\\\\SPARKSQLSERVER:1433;databaseName=spark_db;IntegratedSecurity=true;encrypt=true;trustServerCertificate=true")
    //.option("dbtable", "orders")
    .option("query", "(select state, city , sum(numunits * totalprice) as  total_amount from orders group by state, city)"  ) // with the query selection
    .load()

    df_sqlserver.show(10)

// save file
df_sqlserver.write
  .format("jdbc")
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("url", "jdbc:sqlserver://DESKTOP-O58PAUC\\\\SPARKSQLSERVER:1433;databaseName=spark_db;IntegratedSecurity=true;encrypt=true;trustServerCertificate=true")
  .option("dbtable", "orders_city")

  .save()


  }
}
