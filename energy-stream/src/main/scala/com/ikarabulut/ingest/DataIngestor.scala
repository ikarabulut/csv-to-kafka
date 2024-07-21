package com.ikarabulut.ingest

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

import java.util.Properties

object DataIngestor {
  private val logger = LoggerFactory.getLogger(getClass)

  def ingestData(topic: String, bootstrapServers: String, data: Seq[(String, String)]): Unit = {
    val properties = new Properties
    properties.put("bootstrap.servers", bootstrapServers)
    properties.put("key.serializer", classOf[StringSerializer].getName)
    properties.put("value.serializer", classOf[StringSerializer].getName)
    properties.put("acks", "all")
    properties.put("retries", "3")
    properties.put("compression.type", "snappy")

    val producerCallback: Callback = (_: RecordMetadata, exception: Exception) => Option(exception) match {
      case Some(error) => logger.error(s"Failed to write to topic $topic due to $error")
      case _ => () => // We don't want to log anything for success
    }

    val producer = new KafkaProducer[String, String](properties)

    data.foreach { case (key, value) =>
      val record = new ProducerRecord[String, String](topic, key, value)
      producer.send(record, producerCallback)
    }
  }
}