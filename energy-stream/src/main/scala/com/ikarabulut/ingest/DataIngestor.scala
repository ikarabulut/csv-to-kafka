package com.ikarabulut.ingest

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

import java.util.Properties

object DataIngestor {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val topic = "tariff"
    val bootstrapServers = "localhost:29092"

    val dummyData = (0 to 15).map(i => (s"key$i", s"value$i"))

    ingestData(topic, bootstrapServers, dummyData)
  }

  def ingestData(topic: String, bootstrapServers: String, data: Seq[(String, String)]): Unit = {
    val properties = new Properties()
    properties.put("bootstrap.servers", bootstrapServers)
    properties.put("key.serializer", classOf[StringSerializer].getName)
    properties.put("value.serializer", classOf[StringSerializer].getName)
    properties.put("acks", "all")
    properties.put("retries", "3")
    properties.put("compression.type", "snappy")
    println("CREATING PRODUCER")
    val producer = new KafkaProducer[String, String](properties)
    println("PRODUCER CREATED")

    data.foreach { case (key, value) =>
      val record = new ProducerRecord[String, String](topic, key, value)
      producer.send(record, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (exception != null) {
            println(s"Error sending message with key $key")
            exception.printStackTrace()
          } else {
            println(s"Message sent to topic ${metadata.topic()} on partition ${metadata.partition()} with offset ${metadata.offset()}")
          }
        }
      })
    }
  }
}
