package com.ikarabulut.ingest

import org.json4s.jackson.Serialization.write
import org.json4s.{DefaultFormats, Formats}

import java.sql.Timestamp
import java.text.SimpleDateFormat

case class Tariff(tariffDateTime: Timestamp, tariff: String) {
  implicit val formats: Formats = DefaultFormats.preservingEmptyValues

  def toJSONWithKey: (String, String) = ("", write(this))
}

object Tariff {
  val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm")

  def apply(csvRecords: String): Tariff = {
    val cols = csvRecords.split(",").map(_.trim)
    val tariffDateTime = Timestamp.from(simpleDateFormat.parse(cols(0)).toInstant)
    val tariff = cols(1)
    Tariff(tariffDateTime, tariff)
  }
}
