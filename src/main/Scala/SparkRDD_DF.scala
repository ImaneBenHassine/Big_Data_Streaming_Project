import java.io.FileNotFoundException
import org.apache.log4j.{LogManager, Logger}

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.expressions.Window
import org.apache.hadoop.fs._
import org.apache.spark.sql.expressions.UserDefinedFunction  //transform to UDF

object SparkRDD_DF {
  var ss :SparkSession = null
  var spConf : SparkConf = null

  private var trace_log : Logger = LogManager.getLogger("Logger_Console") // private to this class

  /** ********************************************* Impose Schema ********************************************** */
  val schema_orders = StructType (Array(
     StructField("orderid", IntegerType, false),
     StructField("customerid", IntegerType, true),
     StructField("campaignid", IntegerType, true),
     StructField("orderdate", TimestampType, true),
     StructField("city", StringType, true),
     StructField("state", StringType, true),
     StructField("zipcode", StringType, true),
     StructField("paymenttype", StringType, true),
     StructField("totalprice", StringType, true),
     StructField("numorderlines", IntegerType, true),
     StructField("numunits", IntegerType, true)
  ))

  val schema_orderline = StructType (Array(
     StructField("orderlineid" , IntegerType ,  true)
    ,StructField("orderid" , IntegerType ,  true)
    ,StructField("productid" , IntegerType ,  true)
    ,StructField("shipdate" , TimestampType ,  true)
    ,StructField("billdate" , TimestampType ,  true)
    ,StructField("unitprice" , IntegerType ,  true)
    ,StructField("numunits" , IntegerType ,  true)
    ,StructField("totalprice" , IntegerType ,  true)
  ))

  val schema_product = StructType (Array(
  StructField("PRODUCTID", IntegerType, true)
  , StructField("PRODUCTNAME", StringType, true)
  , StructField("PRODUCTGROUPCODE", StringType, true)
  , StructField("PRODUCTGROUPNAME", StringType, true)
  , StructField("INSTOCKFLAG", StringType, true)
  , StructField("FULLPRICE", IntegerType, true)
  ))


  def main (args: Array[String]): Unit = {
    val session_s = Session_Spark(true)

    // save as UDF
   // session_s.udf.register("valid_phone",valid_phoneUDF)

    val df_test = session_s.read
      .format("com.databriks.spark.csv")
      .option("delimiter",",")
      .option("header","true")
      .csv("C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\Doc\\Data\\2010-12-06.csv")

    //df_test.show(20)

    // read multiple files with same structure
    val df_gp = session_s.read
      .format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\Doc\\Data\\csvs\\")

    println("df_test count :"+ df_test.count() + " df_gp count : " + df_gp.count())
   df_gp.show(5)

 // Read specific files from a repository --> union of df

 val df_gp2 = session_s.read
   .format("csv")
   .option("header", "true")
   .option("inferSchema", "true")
   .load("C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\Doc\\Data\\2010-12-06.csv","C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\Doc\\Data\\2011-01-20.csv")

    //println("df_gp count :" + df_gp.count() + " df_gp2 count : " + df_gp2.count())
    //df_gp2.show(5)

    // df_test.printSchema()



    // Transformation with select not very appreciated
    val df_2 = df_test.select(
      col("InvoiceNo").cast(StringType),
      col("_c0").alias("Id client"),
      col("StockCode").cast(IntegerType).alias("CodeMarchandise"),
      col("Invoice".concat("No")).alias("Id Command")
    )

    //df_2.show(10)
   // df_2.printSchema()

    /*
    // affect transformation to df
    df_test.select(
      col("InvoiceNo").cast(StringType),
      col("_c0").alias("Id client"),
      col("StockCode").cast(IntegerType).alias("CodeMarchandise")
    ).show(4)
*/

   /*********************************************** withColumn ***********************************************/

    val df_3 = df_test.withColumn("InvoiceNo", col("InvoiceNo").cast(StringType))
      .withColumn("StockCode", col("StockCode").cast(IntegerType))
      .withColumn("Coef", lit(50)) // add new column
      .withColumnRenamed("_c0", "Id_Customer")
      .withColumn("Id_Command", concat_ws("_",col("InvoiceNo"), col("Id_Customer")))
      .withColumn("Total_Amount", round(col("UnitPrice") * col("Quantity"),2))
      .withColumn("Created_Date", current_timestamp())
      .withColumn("Reduction", when(col("Total_Amount") > 15, lit(3)).otherwise( lit (0)))
      .withColumn("Reduction_Juin", when(col("Total_Amount") < 15, lit(0))
                                              .otherwise(when(col("Total_Amount").between(15,20), lit(3))
                                              .otherwise(when(col("Total_Amount") >20 ,lit(5)))))
      .withColumn("Net_Income", col("Total_Amount") - col("Reduction"))


    // val df_reduction = df_3.filter(col("Reduction_Juin")=== 0) compare a column to one value , good practice use lit()

    val df_reduction = df_3.filter(col("Reduction_Juin")=== lit(0) && col("Country").isin("Italy", "France"))

   // df_reduction.show(20)



    /*********************************************** Joins ***********************************************/

    val df_orders = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .schema(schema_orders)
      .load("C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\Doc\\Data\\orders.txt")

    // columns with the same name created an ambiguous even if we named them differently because DF works with RDD and we still have the same error : Reference `numunits` is ambiguous, so we need to create another DF

    val df_order_bis = df_orders.withColumnRenamed ("numunits", "numunits_orders")
      .withColumnRenamed("totalprice","totalprice_orders")
      .withColumnRenamed("orderid", "ordersID")

    //df_order_bis.show(10)
    //df_order_bis.printSchema()

    val df_orderline = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .schema(schema_orderline)
      .load("C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\Doc\\Data\\orderline.txt")

    //df_orderline.show(5)
    //df_orderline.printSchema()

    val df_product = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .schema(schema_product)
      .load("C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\Doc\\Data\\product.txt")


    val df_product_bis = df_product.withColumnRenamed ("PRODUCTID", "PRODUCT_ID")

    //df_product_bis.show(5)
    //df_product_bis.printSchema()

    // df_orderline.join(df_orders,"orderid","inner")
    val df_joinOrders = df_orderline.join( df_order_bis, df_orderline.col("orderid") === df_order_bis.col("ordersID"), "Inner")
     .join(df_product_bis, df_product_bis.col("PRODUCT_ID") === df_orderline.col("productid"), "Inner")
      .select(
        col("ordersID"),
        col("city"),
        col("state"),
        col("orderdate"),
        col("orderlineid"),
        col("numunits"),
        col("totalprice"),
        col("PRODUCT_ID"),
        col("PRODUCTGROUPNAME")

      )

    //df_joinOrders.show(5)
    //df_joinOrders.printSchema()

    /*********************************************** Union ***********************************************/
    val df_f1 = session_s.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\Doc\\Data\\2010-12-06.csv")

    val df_f2 = session_s.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\Doc\\Data\\2011-01-20.csv")

    val df_f3 = session_s.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\Doc\\Data\\2011-12-08.csv")

    // val df_union = df_f1.union(df_f2).union (df_f3)
   val df_union = df_f1.union(df_f2.union(df_f3))
  //  println (df_f1.count() +" ,all 3 files counts " + df_union.count())



// group by total amount by city

    df_joinOrders.withColumn("total_amount",round(col("numunits") * col("totalprice"),2))
   .groupBy("state","city")
   .sum("total_amount").as("Total Amount")
    //.show(10)

// Partition by state

    val wn_spec = Window.partitionBy(col("state")).orderBy(col("orderdate").desc)

    val df_window = df_joinOrders.withColumn("sale_depart", sum(round(col("numunits") * col("totalprice"),2))
      .over(wn_spec))
      .select(
        col("ordersID"),
        col("orderlineid"),
        col("orderdate") ,
        col("state") ,
        col("PRODUCTGROUPNAME"),
        col("sale_depart").as("sales by state")
      )
    //df_window.show(5)

    /** ************************************************* SQL  ***************************************************** */
    // create temp view
    df_joinOrders.createOrReplaceTempView("vw_orders")

/*
SQL here is not related to Hive since we are on local not on cluster we just using similar function propre to hive but with normal sql
  now once temp view is created we use it as table for the query select
    session_s.sql("""
      select state, city , sum(round(numunits * totalprice)) as  total_amount from df_joinOrders group by state, city """)
      .show(5)
*/
    val df_sql : DataFrame = session_s.sql(
      """select state, city , sum(round(numunits * totalprice)) as  total_amount from vw_orders group by state, city """)
      //.withColumn() here we can use all the DF functions

      df_sql.show(10)

// save as Hive table (once Hive installed)
   val df_hive = session_s.table("vw_ordres") // read table from metastore Hive
    df_sql.write.mode(SaveMode.Overwrite).saveAsTable("report_orders") // save df in Metastore Hive




/*

    /** ********************************************* Persist DF (on local hard disque) ********************************************** */

   df_window
     .repartition(1) // Having many parts of the file may create problems when deploying in production cause sometimes reporting tools can't load many files so we need to export one file to the final user by using "repartition"
      //.schema( ) to impose schema StructType before writing
     .write
     .format("com.databriks.spark.csv")
     //.mode("overwrite") or "append" but it is safer to use .mode(SaveMode.) ; append works more with streaming when we need to save all previous data
     // by default it saves data with parquet format ans use snappy to compress --> perfct for HDFS but not for a csv ; so need to specify .csv
     .mode(SaveMode.Overwrite)
     .option("header", "true")
     .csv("C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\export\\write")

// fromat ORC
    df_order_bis.write
      .option("orc.bloom.filter.columns", "favorite_color")
      .option("orc.dictionary.key.threshold", "1.0")
      .option("orc.column.encoding.direct", "name")
      .orc("users_with_options.orc")
*/
    /*********************************************** Functions Date & Time & Convert ***********************************************/
    df_order_bis.withColumn("dateNow", date_format(current_date(),"dd/MMMM/yyyy"))
      .withColumn("timeNow", current_timestamp())
      .withColumn("sec_period", window(col("orderdate"), "5 seconds")) // useful when streaming
      .select(
        col("sec_period"),
        col("sec_period.start"),
        col("sec_period.end")
      )
      // .show(10)

       // partition date per days
    df_order_bis.withColumn("dateNow", date_format(current_date(), "dd/MMMM/yyyy"))
      .withColumn("timeNow", current_timestamp())
      .withColumn("day_period", window(col("orderdate"), "5 days"))
      .select(
        col("orderdate"),
        col("day_period"),
        col("day_period.start"),
        col("day_period.end")
      )
     // .show(10)

    // df_union.show(10)
    //df_union.printSchema()
    df_union.withColumn("InvoiceDate", to_date(col("InvoiceDate")))
      .withColumn("InvoiceTimestamp", col("InvoiceTimestamp").cast(TimestampType))  // .totimestamp or .cast
      .withColumn("InvoiceAddMonth", add_months(col("InvoiceDate"),2))
      .withColumn("InvoiceAddDate", date_add(col("InvoiceDate"), 30))
      .withColumn("InvoiceSubDate", date_sub(col("InvoiceDate"), 20))
      .withColumn("InvoiceDiffDate", datediff(current_date(),col("InvoiceDate"))) // nb of days between date and current date
      .withColumn("InvoiceQuarterDate", quarter(col("InvoiceDate")))
      .withColumn("InvoiceDate_ID", unix_timestamp(col("InvoiceDate"))) // gives a serial number to each date--> helps with calculation later when streaming & it gives better precision
      .withColumn("InvoiceDate_format", from_unixtime(unix_timestamp(col("InvoiceDate")),"dd-MM-yyyy" ))
      //.show(10)

    /*********************************************** Functions text & numeric & Regular Expressions ***********************************************/
   df_product
     .withColumn("product_gp", substring(col("PRODUCTGROUPNAME"), 0 , 2))
     .withColumn("product_ln", length(col("PRODUCTGROUPNAME")))
     .withColumn("product_code_name", concat_ws("|", col("PRODUCTGROUPCODE"), col("PRODUCTGROUPNAME")))
     .withColumn("code_minis", lower(col("PRODUCTGROUPCODE")))
     .where(regexp_extract(trim(col("PRODUCTID")),"[0-9]{5}" ,0 ) === trim(col("PRODUCTID"))) // Check if PRODUCTID has 5 characters without counting spaces (trim)
     //.where(col("PRODUCTID").rlike("[0-9]{5}")) // only when matches the expression
     .where( ! col("PRODUCTID").rlike("[0-9]{5}")) // show when not matching the expression
    // .show(10)


    /*********************************************** USER-DEFINED FUNCTION (UDF)  ***********************************************/
  // first example : return a boolean type if phone is valid or not

   def valid_phone(phone_to_test : String) : Boolean =  {

   var result : Boolean = false
   val motif_regex = "^0[0-9]{9}".r  // .r for the Regular expression starts with 0 followed by 9 num

  if (motif_regex.findAllIn(phone_to_test.trim) == phone_to_test.trim ){
    result = true
  } else {
    result =false
  }

  return result
  }
    //transform to UDF

    val valid_phoneUDF : UserDefinedFunction = udf{(phone_to_test : String) => valid_phone(phone_to_test : String) }

    // create df from a list of a num phones
   import session_s.implicits._
    val phone_list : DataFrame= List("0709789450","+3307897025","8794007834").toDF("phone_number")

    phone_list.withColumn("test_phone", valid_phoneUDF(col("phone_number")))
      .show()

// SQL approach with

  phone_list.createOrReplaceTempView("vw_phone")
    session_s.sql("select valid_phone (phone_number) as test_phone from vw_phone ")
      .show()


   def spark_hdfs () : Unit ={

     val config_fs = Session_Spark(true).sparkContext.hadoopConfiguration
     val fs = FileSystem.get(config_fs)

     val src_path = new Path("C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\export\\write")
     val dest_path = new Path("C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\Doc\\Data")
     val src_ren = new Path("Projects\\Scala\\export\\old")
     val src_dest = new Path("Projects\\Scala\\Doc\\new")
     val local_path = new Path ("C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\export\\write\\part.csv")
     val tolocal_path = new Path ("C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\export")

     // list files and their paths from a repository
     val list_file = fs.listStatus(src_path)
     list_file.foreach(f => println(f.getPath))

     val list_file1 = fs.listStatus(src_path).map(x => x.getPath)
     for ( i <-1 to list_file1.length) {
       println(list_file1(i))
     }
     // rename file
   fs.rename(src_ren, src_dest)

   // delete file
     fs.delete(src_dest, true)

     // copy file from local to hdfs or the opposite
     fs.copyFromLocalFile(local_path, dest_path)
     fs.copyToLocalFile(dest_path, tolocal_path)


   }


  }
  //To avoid all those executions every time i put them under mainip_rdd()
   def mainip_rdd(): Unit = {

   // val sc = Session_Spark(env=true).sparkContext
    val sc = Session_Spark(env=true).sparkContext
    val session_s = Session_Spark(true)

    sc.setLogLevel("OFF")   // To deactivate details of trace

   // create RDD from a list
    val rdd : RDD[String] = sc.parallelize(List("my first", "rdd", "via a list"))
     rdd.foreach{
       l=> println(l)
     }
    // create RDD from an array
    val rdd2 : RDD[String] = sc.parallelize(Array ("my second", "rdd", "via an array"))
    rdd2.foreach {
      l => println(l)
    }
    // create RDD from a sequence
    val rdd3 = sc.parallelize(Seq(("my third", "rdd"), ("via an sequence",3)))
        rdd3.take(num = 1).foreach {l => println(l)}

    if (rdd3.isEmpty()) {
      println("RDD is empty")
    } else {
      rdd3.foreach {l => println(l)}
    }
/*
    // save RDD as a file
  rdd3.saveAsTextFile("C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\export\\rdd3.txt")

    // decide the number of partition
    rdd3.repartition(numPartitions = 1).saveAsTextFile("C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\export\\rdd3_parti1.txt")

   rdd3.collect().foreach {l => println(l)} // create a file on the driver

    create an RDD from a text file
  val  rdd_txt = sc.textFile("C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\export\\rdd_test.txt")
    println("reading from a text file")
    rdd_txt.foreach {l => println(l)}

 //create an RDD from multiple text files
 val  rdd5 = sc.textFile("C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\export\\*")
    println("reading from multiple text files")
    rdd5.foreach { l => println(l) }
*/
/*********************************************   transformation of RDD .map ; .flatMap *****************************************/
 val rdd_trans  : RDD[String] = sc.parallelize(List("Scala combines object-oriented and functional programming in one concise, high-level language","Scala's static types help avoid bugs in complex applications" ,"and its JVM and JavaScript runtimes let you build high-performance systems with easy access to huge ecosystems of libraries."))
   rdd_trans.foreach(l=> println("row of my RDD :"+l))

    val rdd_map = rdd_trans.map(x=> x.split(" "))

    println("nb of elements in my RDD "+ rdd_map.count())

// count nb of character for each element
    val rdd_t2 = rdd_trans.map(w => (w, w.length))
    rdd_t2.foreach(l=> println(l))

/* add a third column to check if word Scala exists */
    val rdd_t3 = rdd_trans.map(w => (w, w.length, w.contains("Scala")))
    rdd_t3.foreach(l => println(l))

/* uppercase each elements and inverse order */
    val rdd_t4 = rdd_t3.map(x => (x._1.toUpperCase(),x._3, x._2))
    rdd_t4.foreach(l => println(l))

/* for each column splitted add value 1 --> here .map is limited */
    val rdd_t5 = rdd_t3.map(x => (x._1.split(" "),1))
    rdd_t5.foreach(l => println(l))

// here .flatMap did the job of splitting better than .map
    val rdd_t6 = rdd_trans.flatMap(x => x.split (" ")).map ( w=> (w,1))
    rdd_t6.foreach(l => println(l))

    val rdd_t7 = rdd_trans.flatMap(x=> x.split (" ")).map( w=> (w,1))
    // rdd_t7.repartition(1).saveAsTextFile("C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\export\\rdd_t7.txt")
      //.foreach(l => println(l))

    /*********************************************   Filter *****************************************/

 val rdd_fil = rdd_t7.filter(x => x._1.equals("Scala"))
    rdd_fil.foreach(l => println(l))

    val rdd_fil2 = rdd_t7.filter(x => x._1.contains("jvm"))
    rdd_fil2.foreach(l => println(l))

  val rdd_reduced =  rdd_t7.reduceByKey((x,y) => (x+y))
    rdd_reduced.foreach(l => println(l))

 // Count nb of repetitions for each element and save it in a partition.

    val rdd_t8 = rdd_trans.flatMap(x=> x.split (" ")).map( w=> (w,1))reduceByKey((x,y) => (x+y))
    //rdd_t8.repartition(1).saveAsTextFile("C:\\Users\\MonPC\\Desktop\\01-ImenBH\\Projects\\Scala\\export\\rdd_t8.txt")

    /************************************** RDD to Dataframe ***********************/

    import session_s.implicits._
    val df : DataFrame =  rdd_t7.toDF("text","value")
   // df.show(10)


  }

  /** *******************************************   Dataframe   **************************************** */
  // from session unlike RDD from context

  /* syntaxe 1
 val ss1 : SparkSession= SparkSession.builder()
   .appName("My First Spark")
   .config("spark.serializer","org.apache.serialize.KryoSerializer")
   .config("spark.sql.crossJoin.enabled","true")
   //.enableHiveSupport()
   .getOrCreate()
*/
  /* syntaxe 2 : recommended to create a function */

  /**
   * function that initializes and instantiates a spark session
   *
   * @param env this is a variable that indicates the environment on which our application is deployed.
   *     If env = True, then the app is deployed locally, otherwise it is deployed on a cluster
   */
  def Session_Spark (env: Boolean = true): SparkSession = {
    try {
      if (env == true) {
        System.setProperty("hadoop.home.dir", "C:/Users/MonPC/Desktop/01-ImenBH/Projects/PySpark/Hadoop") // logging if winutils not found
        ss = SparkSession.builder
          .master("local[*]")
          .config("spark.sql.crossJoin.enabled", "true")
          //   .enableHiveSupport()
          .getOrCreate()
      } else {
        ss = SparkSession.builder
          .appName("Mon application Spark")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.sql.crossJoin.enabled", "true")
          //.enableHiveSupport()
          .getOrCreate()
      }
    } catch {
      case ex: FileNotFoundException => trace_log.error("winutils of Haddop not found on the path  " + ex.printStackTrace())
      case ex: Exception => trace_log.error("Error with initialisation of the Spark session " + ex.printStackTrace())
    }

    return ss

  }

}



