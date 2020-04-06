import java.util

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.JavaConverters._

object droneAlertHandler {



    def initializeConsumer(): KafkaConsumer[String, String] ={

      val props = new Properties()

      props.put("bootstrap.servers", "localhost:9092")

      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

      props.put("auto.offset.reset", "latest")

      props.put("group.id", "consumer-group")




      val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)

      consumer.subscribe(util.Arrays.asList("sendDroneData"))
      return  consumer
    }

    def initializeKafka(): KafkaProducer[String,String] = {
      val props = new Properties()

      props.put("bootstrap.servers", "localhost:9092")

      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")

      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

      val producer = new KafkaProducer[String, String](props)
      return  producer
    }


    def writeToKafka(producer: KafkaProducer[String,String],key: Int,value: String): Unit = {

      val record = new ProducerRecord[String, String]("sendToFlask", key.toString, value)

      producer.send(record)


    }


    def consumeFromKafkaToDF(producer: KafkaProducer[String,String],consumer: KafkaConsumer[String,String], record: Iterable[ConsumerRecord[String,String]],cter:Int):Unit = {
      val record = consumer.poll(1000000000).asScala

      if (record.iterator.hasNext) {
        val row = record.iterator.next().value().toString
        val test = row.split(",",-1).toList
        if( test(0) == "imageNotClear"|| test(0) == "lowBattery") {

          writeToKafka(producer,cter,row)
        }
        consumeFromKafkaToDF(producer,consumer,record,cter)
      }


    }

    def main(args: Array[String]): Unit = {
      val producer = initializeKafka()
      val consumer = initializeConsumer()
      consumeFromKafkaToDF(producer,consumer,null,0)

    }








}
