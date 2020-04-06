// Databricks notebook source
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{Column, DataFrame, SQLContext, SparkSession}
import com.mongodb.spark.MongoSpark
import org.apache.log4j.{Level, Logger}


object analysis {


  def main(args: Array[String]): Unit = {
    // COMMAND ----------
   Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setAppName("project")
      .setMaster("local[*]") // here local mode. And * means you will use as much as you have cores.


    val ss = SparkSession.builder()
      .master("local")
      .appName("project")
      .config(conf)
      .config("spark.mongodb.input.readPreference.name", "secondaryPreferred")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/prestaCop.historicalData")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/prestaCop.historicalData")
      .getOrCreate()

    // COMMAND ----------

    val df = MongoSpark.load(ss)


    // COMMAND ----------

    val colsToRemove = Seq("BIN", "BBL", "NTA", "Census Tract"
      , "Community Council", "Community Board", "Longitude", "Latitude"
      , "Double Parking Violation", "Hydrant Violation"
      , "Vehicle Year", "Meter Number", "Feet From Curb", "To Hours In Effect"
      , "Days Parking In Effect", "From Hours In Effect", "Sub Division", "Law Section"
      , "Date First Observed", "Intersecting Street", "Violation In Front Of Or Opposite", "Violation County"
      , "Time First Observed", "Issuer Command", "Issuer Squad", "Issuer Code")

    val dfs = df.select(df.columns.filter(colName => !colsToRemove.contains(colName)).map(colName => new Column(colName)): _*)
    /* on databricks

    display(dfs)
     */
    // COMMAND ----------

    //display(dfs)

    // COMMAND ----------

    val d_state =  dfs.groupBy("Registration State").count().show()
    /*** Number of violations by state ***/

    // COMMAND ----------

    val d_plate = dfs.groupBy("Plate ID").count()
    d_plate.where("count > 3").where("count != 39").where("count != 12").show()
    /*** Number of violations by plate ID ***/

    // COMMAND ----------

    val d_veh = dfs.groupBy("Vehicle Make", "Vehicle Color").count().show()
    /*** Number of violations by Vehicle make ***/

    // COMMAND ----------
    /*** Number of violations by Vehicle color ***/

    // COMMAND ----------

    val d_vio = df.groupBy("Violation Precinct").count()
    d_vio.where("count > 300").show()
    /*** Number of violations above 300 by Violation Precinct ***/

    // COMMAND ----------

    val d_iss = df.groupBy("Issuer Precinct").count()
    d_iss.where("count > 300").show()
    /*** Number of violations above 300 by Issuer Precinct ***/

    /* on databricks

    display(dfs)
    display(dfs.groupBy("Registration State").count())
    val d = dfs.groupBy("Plate ID").count()
    display(d.where("count > 3").where("count != 39").where("count != 12"))
    val d = dfs.groupBy("Vehicle Make", "Vehicle Color").count()
    display(d)
    val d = df.groupBy("Violation Precinct").count()
    display(d.where("count > 300"))
    val d = df.groupBy("Issuer Precinct").count()
    display(d.where("count > 300"))

     */



  }

}
