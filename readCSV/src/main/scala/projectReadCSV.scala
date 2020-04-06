import java.util.Properties

import kafka.server.KafkaApis
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source
import scala.util.parsing.json.JSON

object projectReadCSV {

  case class Hist(plateId: String, registrationState: String, plateType: String, issueDate: String,violationCode: String, vehicleBodyType: String, streetCode1: String,
                  streetCode2: String,streetCode3: String, vehicleYear: String)

  def initializeKafka(): KafkaProducer[String,String] = {
    val props = new Properties()

    props.put("bootstrap.servers", "localhost:9092")

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    return  producer
  }
  def writeToKafka(producer: KafkaProducer[String,String],key: Int,value: String): Unit = {

    val record = new ProducerRecord[String, String]("sendHist", key.toString, value)

    producer.send(record)


  }

  def sendOneByOne(producer: KafkaProducer[String,String],cter: Int,lines:Iterator[String]):Unit = {
    if(lines.hasNext){
      val line = lines.next().mkString
      writeToKafka(producer,cter,line)
      sendOneByOne(producer,cter+1,lines)

    }
  }
  def main(args: Array[String]): Unit = {
    println("------------------------------------")
    println("Initialize producer")
    val producer = initializeKafka();

    println("Sending first csv")
    val path = "src\\data\\Parking_Violations_Issued_-_Fiscal_Year_2014__August_2013___June_2014_.csv"
    val lines = Source.fromFile(path).getLines.drop(1)
    sendOneByOne(producer,0,lines)

    println("Sending second csv")
    val path2 = "src\\data\\Parking_Violations_Issued_-_Fiscal_Year_2015.csv"
    val lines2 = Source.fromFile(path2).getLines.drop(1)
    sendOneByOne(producer,0,lines2)

    println("Sending third csv")
    val path3 = "src\\data\\Parking_Violations_Issued_-_Fiscal_Year_2016.csv"
    val lines3 = Source.fromFile(path3).getLines.drop(1)
    sendOneByOne(producer,0,lines3)

    println("Sending last csv")
    val path4 = "src\\data\\Parking_Violations_Issued_-_Fiscal_Year_2017.csv"
    val lines4 = Source.fromFile(path4).getLines.drop(1)
    sendOneByOne(producer,0,lines4)

    println("Done")
    producer.close()
    println("------------------------------------")

  }
}
