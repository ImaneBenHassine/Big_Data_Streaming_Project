import SparkRDD_DF._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
object UseCase_BANO {

  // create schema for the file
  val schema_bano = StructType (Array(
    StructField("id_bano", StringType, false),
    StructField("num_voie", StringType, false),
    StructField("nom_voie", StringType, false),
    StructField("code_postal", StringType, false),
    StructField("nom_commune", StringType, false),
    StructField("code_source_bano", StringType, false),
    StructField("latitude", StringType, true),
    StructField("longitude", StringType, true)
  ))

  //create new config hadoop
  val configH = new Configuration()
  val fs = FileSystem.get(configH)
  val path_dest = new Path("C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\Doc\\Data\\BANO\\destination")
  def main (args: Array[String]): Unit = {

    val ss =Session_Spark(true)

    val df_bano_brut = ss.read
      .format("com.databriks.spark.csv")
      .option("delimiter",",")
      .option("header", "true")
      .schema(schema_bano)
      .csv("C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\Doc\\Data\\BANO\\full.csv")

    //df_bano_brut.show(10)

    val df_bano = df_bano_brut
      .withColumn("code_depart", substring(col("code_postal"),1,2))
      .withColumn("source", when (col("code_source_bano") === lit("OSM"),lit("OpenStreetMap")) // lit to compare a column with a value to avoid error
        .otherwise(when (col("code_source_bano") === lit("OD"),lit("OpenData"))
        .otherwise(when (col("code_source_bano") === lit("O+O"),lit("OpenData OSM"))
        .otherwise(when (col("code_source_bano") === lit("CAD"),lit("Cadastre"))
        .otherwise(when (col("code_source_bano") === lit("C+O"),lit("Cadastre OSM")))))))

   df_bano.show(10)


    // create a new file for ea ch department
/* manipulate a dataframe (RDD) */
    val df_depart = df_bano.select(col("code_depart"))
      .distinct()
      .filter(col("code_depart").isNotNull)
       // Since the list is not an RDD, it cannot be partitioned so there is no "tolist" from a df --> we use .collect

/* manipulate a list Linear execution without pararlization.
     Since the list is not an RDD, it cannot be partitioned so there is no "tolist" from a df --> we use .collect : retrieves data and transform it to the driver
     .toList no yoList exist but only for one element one row so we use .map or .flatmap
     filter on the department number to control the number of files generated
    */

    val list_depart = df_bano.select(col("code_depart"))
      .distinct()
      .filter(col("code_depart").isin("01","35","07"))
      .collect()
      .map(x => x(0)).toList

     list_depart.foreach( e => println(e.toString))

  /* Method  1 : using list --> I loop a scala list and for every element of the list, I execute the filter in the machine.
    - action replace () --> {}
    - coalesce : so the calculation is performed on the main node & impose number of file (more like partition)
    --> here we create 97 files separately in one action --> no distribution --> scala classic
    process takes 4 min to generate 3 files and move them  --> since it was not parallelized --> if it was on cluster with df script it will be faster

*/
    list_depart.foreach{
      x => df_bano.filter(col("code_depart") === x.toString)
        .coalesce(1)
        .write
        .format("com.databricks.spark.csv")
        .option("header","true")
        .option("delimiter", ";")
        .mode (SaveMode.Overwrite)
        .csv("C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\Doc\\Data\\BANO\\bano" + x.toString)

       val path_src = new Path("C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\Doc\\Data\\BANO\\bano" + x.toString)

        fs.copyFromLocalFile(path_src, path_dest)
    }

    /* Method  2 : using DF
    - repartition : to have only one file for each department
    --> this is distribution with spark --> no we need to test performance
 */
   df_depart.foreach {
     dep => df_bano.filter(col("code_depart") === dep.toString)
         .repartition(1)
         .write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .option("delimiter", ";")
         .mode(SaveMode.Overwrite)
         .csv("C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\Doc\\Data\\BANO\\bano" + dep.toString)

       // Now we move the files to repository source using hdfs client method fs
       val path_src = new Path("C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\Doc\\Data\\BANO\\bano"+ dep.toString)

       fs.copyFromLocalFile(path_src, path_dest)
   }









  }












}
