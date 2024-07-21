package com.ikarabulut.ingest

import io.circe.{Encoder, Decoder, Json}
import io.circe.syntax._
import io.circe.parser._
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.Instant

case class Tariff(tariffDateTime: Timestamp, tariff: String) {

  def toJSONWithKey: (String, String) = {
    val json = this.asJson.noSpaces
    ("", json)
  }
}

object Tariff {
  val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm")

  implicit val timestampEncoder: Encoder[Timestamp] = Encoder.encodeString.contramap[Timestamp](_.toInstant.toString)
  implicit val timestampDecoder: Decoder[Timestamp] = Decoder.decodeString.emapTry { str =>
    scala.util.Try(Timestamp.from(Instant.parse(str)))
  }

  implicit val tariffEncoder: Encoder[Tariff] = Encoder.forProduct2(
    "tariffDateTime",
    "tariff"
  )(t => (t.tariffDateTime, t.tariff))

  implicit val tariffDecoder: Decoder[Tariff] = Decoder.forProduct2(
    "tariffDateTime",
    "tariff"
  )(Tariff.apply)

  def apply(csvRecords: String): Tariff = {
    val cols = csvRecords.split(",").map(_.trim)
    val tariffDateTime = Timestamp.from(simpleDateFormat.parse(cols(0)).toInstant)
    val tariff = cols(1)
    Tariff(tariffDateTime, tariff)
  }
}
