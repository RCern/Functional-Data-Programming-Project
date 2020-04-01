import java.util

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import java.util.Properties

import scala.collection.JavaConverters._
import org.mongodb.scala._
import java.time.LocalDateTime



object consumer {



  def initializeConsumer(): KafkaConsumer[String, String] ={

    val props = new Properties()

    props.put("bootstrap.servers", "192.168.1.24:9092")

    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    props.put("auto.offset.reset", "latest")

    props.put("group.id", "consumer-group")

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)

    consumer.subscribe(util.Arrays.asList("sendHist"))
    return  consumer
  }

  def dumpToDb(collection: MongoCollection[Document],row: List[String]):Unit  ={
    if(row.size == 51) {
        val doc: Document = Document("Summons Number" -> row(0), "Plate ID" -> row(1), "Registration State" -> row(2), "Plate Type" -> row(3), "Issue Date" -> row(4), "Violation Code" -> row(5), "Vehicle Body Type" -> row(6),
          "Vehicle Make" -> row(7), "Issuing Agency" -> row(8), "Street Code1" -> row(9), "Street Code2" -> row(10), "Street Code3" -> row(11), "Vehicle Expiration Date" -> row(12), "Violation Location" -> row(13), "Violation Precinct" -> row(14),
          "Issuer Precinct" -> row(15), "Issuer Code" -> row(16), "Issuer Command" -> row(17), "Issuer Squad" -> row(18), "Violation Time" -> row(19), "Time First Observed" -> row(20), "Violation County" -> row(21), "Violation In Front Of Or Opposite" -> row(21),
          "House Number" -> row(23), "Street Name" -> row(24), "Intersecting Street" -> row(25), "Date First Observed" -> row(26), "Law Section" -> row(27), "Sub Division" -> row(28), "Violation Legal Code" -> row(29), "Days Parking In Effect" -> row(30), "From Hours In Effect" -> row(31),
          "To Hours In Effect" -> row(32), "Vehicle Color" -> row(33), "Unregistered Vehicle?" -> row(33), "Vehicle Year" -> row(34), "Meter Number" -> row(35), "Feet From Curb" -> row(36), "Violation Post Code" -> row(37), "Violation Description" -> row(38), "No Standing or Stopping Violation" -> row(39),
          "Hydrant Violation" -> row(40), "Double Parking Violation" -> row(41), "Latitude" -> row(42), "Longitude" -> row(43), "Community Board" -> row(44), "Community Council" -> row(46), "Census Tract" -> row(47), "BIN" -> row(48), "BBL" -> row(49), "NTA" -> row(50))
        val observable = collection.insertOne(doc)
        observable.subscribe(new Observer[Completed] {
          override def onNext(result: Completed): Unit = println(s"onNext: $result")
          override def onError(e: Throwable): Unit = println(s"onError: $e")
          override def onComplete(): Unit = println("onComplete")
        })
    }
    if(row.size == 43){
        val doc: Document = Document("Summons Number" -> row(0), "Plate ID" -> row(1), "Registration State" -> row(2), "Plate Type" -> row(3), "Issue Date" -> row(4), "Violation Code" -> row(5), "Vehicle Body Type" -> row(6),
          "Vehicle Make" -> row(7), "Issuing Agency" -> row(8), "Street Code1" -> row(9), "Street Code2" -> row(10), "Street Code3" -> row(11), "Vehicle Expiration Date" -> row(12), "Violation Location" -> row(13), "Violation Precinct" -> row(14),
          "Issuer Precinct" -> row(15), "Issuer Code" -> row(16), "Issuer Command" -> row(17), "Issuer Squad" -> row(18), "Violation Time" -> row(19), "Time First Observed" -> row(20), "Violation County" -> row(21), "Violation In Front Of Or Opposite" -> row(21),
          "House Number" -> row(23), "Street Name" -> row(24), "Intersecting Street" -> row(25), "Date First Observed" -> row(26), "Law Section" -> row(27), "Sub Division" -> row(28), "Violation Legal Code" -> row(29), "Days Parking In Effect" -> row(30), "From Hours In Effect" -> row(31),
          "To Hours In Effect" -> row(32), "Vehicle Color" -> row(33), "Unregistered Vehicle?" -> row(33), "Vehicle Year" -> row(34), "Meter Number" -> row(35), "Feet From Curb" -> row(36), "Violation Post Code" -> row(37), "Violation Description" -> row(38), "No Standing or Stopping Violation" -> row(39),
          "Hydrant Violation" -> row(40), "Double Parking Violation" -> row(41))
        val observable = collection.insertOne(doc)
        observable.subscribe(new Observer[Completed] {
          override def onNext(result: Completed): Unit = println(s"onNext: $result")
          override def onError(e: Throwable): Unit = println(s"onError: $e")
          override def onComplete(): Unit = println("onComplete")
        })}
      else {
        println("Error")
    }
  }



  def consumeFromKafkaToDF(collection: MongoCollection[Document], consumer: KafkaConsumer[String,String], record: Iterable[ConsumerRecord[String,String]]):Unit = {
    val record = consumer.poll(10000).asScala

    if (record.iterator.hasNext) {


        val row = record.iterator.next().value().toString.split(",",-1).toList
        dumpToDb(collection,row)
        println(row.mkString)

        consumeFromKafkaToDF(collection,consumer,record)
      }


  }



    def main(args: Array[String]): Unit = {

      val mongoClient: MongoClient = MongoClient("mongodb://127.0.0.1:27017/")
      val database: MongoDatabase = mongoClient.getDatabase("prestaCop")
      val collection: MongoCollection[Document] =database.getCollection("historicalData");
      val consumer = initializeConsumer()

      consumeFromKafkaToDF(collection,consumer,null)

    }



}
