import SparkRDD_DF.ss
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest._
import org.apache.spark.sql.functions.lit

import com.holdenkarau.spark.testing._


// trait :because we can't call the spark session inside the test class. then we extends the class with the trait using "with"
trait SparkSessionProvider  {
  val sst = SparkSession.builder
    .master("local[*]")
    .getOrCreate()
}
// first test for the Spark Session
class SparkTestUnit extends AnyFlatSpec with SparkSessionProvider with DataFrameSuiteBase {

  it should("Instantiate a Spark Session") in {
    var env : Boolean = true
    val sst = SparkRDD_DF.Session_Spark(env)
  }
  // testing the DF
  it should ("compare two dataframe") in {
    val structure_df = List (
      StructField("employee",StringType, true),
      StructField("salary",IntegerType, true)
      )
    val data_df = Seq (
      Row("imen", 140000),
      Row("ala", 200000),
      Row("zakariya", 150000)
    )
    val df_src : DataFrame = sst.createDataFrame(
      sst.sparkContext.parallelize(data_df),
      StructType(structure_df)
    )
    df_src.show(3)

    val df_new : DataFrame = df_src.withColumn("salary", lit(100000)) // "lit" to affect a value to a column
    df_new.show()

    //assert(df_src.columns.size === df_new.columns.size) // test if df have the same column number
    // assert(df_src.count() === 3)

    // assert(df_src.take(3).length === 15) // with "take" spark go fetch , take data from the Master , all data passes throw the driver --> causes memory saturation

//    assertDataFrameEquals(df_src, df_new)


  }

}
