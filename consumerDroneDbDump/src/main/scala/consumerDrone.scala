import java.util

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import java.util.Properties

import scala.collection.JavaConverters._
import org.mongodb.scala._

object consumerDrone {



    def initializeConsumer(): KafkaConsumer[String, String] ={

      val props = new Properties()

      props.put("bootstrap.servers", "localhost:9092")

      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

      props.put("auto.offset.reset", "latest")

      props.put("group.id", "consumer-group-1")

      val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)

      consumer.subscribe(util.Arrays.asList("sendDroneData"))
      return  consumer
    }

    def dumpToDb(collection: MongoCollection[Document],row: List[String]):Unit  ={

        val doc: Document = Document("Type" -> row(0), "ViolationCode" -> row(1), "ViolationDescription" -> row(2), "Timestamp" -> row(3), "ImageId" -> row(4),"Location"->row(5),"DroneId"->row(6))
        val observable = collection.insertOne(doc)
        observable.subscribe(new Observer[Completed] {
          override def onNext(result: Completed): Unit = println(s"onNext: $result")
          override def onError(e: Throwable): Unit = println(s"onError: $e")
          override def onComplete(): Unit = println("onComplete")
        })

    }



    def consumeFromKafkaToDF(collection: MongoCollection[Document], consumer: KafkaConsumer[String,String], record: Iterable[ConsumerRecord[String,String]]):Unit = {
      val record = consumer.poll(1000000).asScala

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
      val collection: MongoCollection[Document] =database.getCollection("droneData")
      val consumer = initializeConsumer()

      consumeFromKafkaToDF(collection,consumer,null)

    }








}
