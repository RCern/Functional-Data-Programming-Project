import java.time.LocalDateTime

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import java.time.format.DateTimeFormatter
import java.util.Properties

import breeze.linalg.*
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.functions.rand
import org.apache.spark
import org.apache.spark.util.random

object droneSim {

  val pathToFile = "src\\main\\data\\"

  def loadData(): DataFrame = {
    // create spark configuration and spark context: the Spark context is the entry point in Spark.
    // It represents the connexion to Spark and it is the place where you can configure the common properties
    // like the app name, the master url, memories allocation...
    val conf = new SparkConf()
                        .setAppName("project")
                        .setMaster("local[*]") // here local mode. And * means you will use as much as you have cores.

    val ss = SparkSession.builder()
      .appName("project")
      .config(conf)
      .getOrCreate()

    val vioSchema = new StructType()
      .add("Summons Number", IntegerType).add("Plate ID", StringType).add("Regis  tration State", StringType)
      .add("Plate Type", StringType).add("Issue Date", StringType).add("Violation Code", StringType)
      .add("Vehicle Body Type", StringType).add("Vehicle Make", StringType).add("Issuing Agency", StringType)
      .add("Street Code1", IntegerType).add("Street Code2", IntegerType).add("Street Code3", IntegerType)
      .add("Vehicle Expiration Date", StringType).add("Violation Location", IntegerType)
      .add("Violation Precinct", IntegerType).add("Issuer Precinct", IntegerType).add("Issuer Code", StringType)
      .add("Issuer Command", StringType).add("Issuer Squad", StringType).add("Violation Time", StringType)
      .add("Time First Observed", StringType).add("Violation County", StringType).add("Violation In Front Of Or Opposite", StringType)
      .add("House Number", StringType).add("Street Name", StringType).add("Intersecting Street", StringType)
      .add("Date First Observed", StringType).add("Law Section", IntegerType).add("Sub Division", StringType)
      .add("Violation Legal Code", StringType).add("Days Parking In Effect", StringType).add("From Hours In Effect", StringType)
      .add("To Hours In Effect", StringType).add("Vehicle Color", StringType).add("Unregistered Vehicle?", StringType)
      .add("Vehicle Year", StringType).add("Meter Number", StringType).add("Feet From Curb", StringType)
      .add("Violation Post Code", StringType).add("Violation Description", StringType)
      .add("No Standing or Stopping Violation", StringType).add("Hydrant Violation", StringType)
      .add("Double Parking Violation", StringType).add("Latitude", StringType).add("Longitude", StringType)
      .add("Community Board", StringType).add("Community Council", StringType)
      .add("Census Tract", StringType).add("BIN", StringType).add("BBL", StringType).add("NTA", StringType)


    val violations2014= ss.read.schema(vioSchema).csv(pathToFile+"Parking_Violations_Issued_-_Fiscal_Year_2014__August_2013___June_2014_.csv").toDF()
    val violations2015= ss.read.schema(vioSchema).csv(pathToFile+"Parking_Violations_Issued_-_Fiscal_Year_2015.csv").toDF()
    val violations2016= ss.read.schema(vioSchema).csv(pathToFile+"Parking_Violations_Issued_-_Fiscal_Year_2016.csv").toDF()
    val violations2017= ss.read.schema(vioSchema).csv(pathToFile+"Parking_Violations_Issued_-_Fiscal_Year_2017.csv").toDF()
    val dfs = violations2017.union(violations2014).union(violations2015).union(violations2016)
    val colsToRemove = Seq("BIN", "BBL", "NTA", "Census Tract"
                          , "Community Council", "Community Board", "Longitude", "Latitude"
                          , "Double Parking Violation", "Hydrant Violation", "Violation Post Code"
                          , "Vehicle Year", "Meter Number", "Feet From Curb", "To Hours In Effect"
                          , "Days Parking In Effect", "From Hours In Effect", "Sub Division", "Law Section"
                          , "Date First Observed", "Intersecting Street", "Violation In Front Of Or Opposite", "Violation County"
                          , "Time First Observed", "Issuer Command", "Issuer Squad", "Issuer Code")

   dfs.select(dfs.columns .filter(colName => !colsToRemove.contains(colName)) .map(colName => new Column(colName)): _*)


  }

  /***

  drone simulator:

    two functionality:

  ****| send standard msg: (location, time, droneID)
  if violation, send: (violation code, nature of offense, imgID)

  set a timer on which msg will be sent, every random(10, 50) msg is violation


  ****| Alert: if img not clear
  send an alert every random (100, 150) msg

  ***/
  def initializeKafka(): KafkaProducer[String,String] = {
    val props = new Properties()

    props.put("bootstrap.servers", "192.168.1.24:9092")

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    return  producer
  }
  def writeToKafka(producer: KafkaProducer[String,String],key: Int,value: String): Unit = {

    val record = new ProducerRecord[String, String]("sendDroneData", key.toString, value)

    producer.send(record)


  }


  def randomInt1to100 = scala.util.Random.nextInt(100)+1

  def violation = scala.util.Random.nextInt( (50 - 10)+ 1)

  def alert = 100 + scala.util.Random.nextInt(( 150 - 100) + 1)

  def drone(producer: KafkaProducer[String,String],cter:Int,location: DataFrame, Violation_Code: DataFrame, Violation_Description: DataFrame, n: Int, imgID: Int, s: Int):
  Option[Any] = (location, Violation_Code, Violation_Description, n, imgID, randomInt1to100) match{

    //violation
    case _ if n % 3 == 0  =>
      val vio = Violation_Code.orderBy(rand()).limit(1).collect().map(_.getString(0)).mkString(" ")
      val vioDesc = Violation_Description.orderBy(rand()).limit(1).collect().map(_.getString(0)).mkString(" ")
      val time = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm").format(LocalDateTime.now)
      val loca = location.orderBy(rand()).limit(1).collect.map(row=>row.toString).mkString(" ").replaceAll("[,\\[\\]]","")
      val droneID = randomInt1to100
      println("Violation ===> "+"Violation: "+vio+ ", Description: " + vioDesc+ ", Time: "+ time.toString+", Image ID: "+ imgID.toString)
      val violation = "violation," + vio + "," + vioDesc + "," + time.toString + "," + imgID.toString + "," + loca + "," + droneID
      writeToKafka(producer,cter,violation)
      drone(producer,cter,location, Violation_Code, Violation_Description, n + 1, imgID + 1, randomInt1to100)
    //val appended = firstDF.union(newRow.toDF())Some(h)

    //alert img
    case _ if n % 7 == 0  =>
      val time = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm").format(LocalDateTime.now)
      val loca = location.orderBy(rand()).limit(1).collect.map(row=>row.toString).mkString(" ").replaceAll("[,\\[\\]]","")
      val droneID = randomInt1to100
      val violation = "imageNotClear, "  + ", " +   "," + time.toString + "," + imgID.toString + "," +loca + ","+ droneID
      writeToKafka(producer,cter,violation)
      drone(producer,cter,location, Violation_Code, Violation_Description, n + 1, imgID + 1, randomInt1to100)

    //alert battery
    case _ if randomInt1to100 < 20  =>
      println("Alert ===> Battery low !!!! ")
      val time = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm").format(LocalDateTime.now)
      val loca = location.orderBy(rand()).limit(1).collect.map(row=>row.toString).mkString(" ").replaceAll("[,\\[\\]]","")
      val droneID = randomInt1to100
      val violation = "lowBattery, "  + ", " +   "," + time.toString + "," + imgID.toString + "," +loca + "," + droneID
      writeToKafka(producer,cter,violation)
      drone(producer,cter,location, Violation_Code, Violation_Description, n + 1, imgID + 1, randomInt1to100)

    // end
    case _ if n > 500 =>
      println("Finished")
      None

    // standard
    case _  =>
      val vio = Violation_Code.orderBy(rand()).limit(1).collect().map(_.getString(0)).mkString(" ")
      val vioDesc = Violation_Description.orderBy(rand()).limit(1).collect().map(_.getString(0)).mkString(" ")
      val time = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm").format(LocalDateTime.now)
      val loca = location.orderBy(rand()).limit(1).collect.map(row=>row.toString).mkString(" ").replaceAll("[,\\[\\]]","")
      println("Standard ===> "+"Location: "+loca+", Time: "+time.toString+", Drone ID: "+randomInt1to100.toString)
      val droneID = randomInt1to100

      val standard = "normal,"  + vio + "," + vioDesc + "," + time.toString + "," + imgID.toString + "," + loca + "," + droneID
      writeToKafka(producer,cter,standard)


      drone(producer,cter,location, Violation_Code, Violation_Description, n + 1, imgID + 1, randomInt1to100)

  }


  def main(args: Array[String]): Unit = {
    println("------------------------------------")
    val producer = initializeKafka()
    val df = loadData()
    df.show()
    println("*************************************************************************************")

    val location = df.filter(df.col("House Number").isNotNull && df.col("Street Name").isNotNull).select("House Number", "Street Name").distinct()
    val Violation_Code = df.filter(df.col("violation code").isNotNull).select("violation code").distinct()
    val Violation_Description = df.filter(df.col("Violation Description").isNotNull).select("Violation Description").distinct()
    // send a message every 5 sec
    val t = new java.util.Timer()
    val task = new java.util.TimerTask {
      def run() = println("Beep!")
      drone(producer,0,location, Violation_Code, Violation_Description, 1, 1, randomInt1to100)
    }
    t.schedule(task, 1000L, 1000L)
    task.cancel()
    println("------------------------------------")

  }
}
