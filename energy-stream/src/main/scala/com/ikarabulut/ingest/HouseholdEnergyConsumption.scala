package com.ikarabulut.ingest

import io.circe.{Encoder, Decoder, Json}
import io.circe.syntax._
import io.circe.parser._
import java.sql.Timestamp
import java.time.{Duration, Instant}

case class HouseholdEnergyConsumption(householdId: String,
                                      stdOrToU: String,
                                      energyReadingTime: Timestamp,
                                      energyConsumption: Option[Double]) {

  def toJSONWithKey: (String, String) = {
    val json = this.asJson.noSpaces
    (this.householdId, json)
  }
}

object HouseholdEnergyConsumption {
  implicit val timestampEncoder: Encoder[Timestamp] = Encoder.encodeString.contramap[Timestamp](_.toInstant.toString)
  implicit val timestampDecoder: Decoder[Timestamp] = Decoder.decodeString.emapTry { str =>
    scala.util.Try(Timestamp.from(Instant.parse(str)))
  }

  implicit val householdEnergyConsumptionEncoder: Encoder[HouseholdEnergyConsumption] = Encoder.forProduct4(
    "householdId",
    "stdOrToU",
    "energyReadingTime",
    "energyConsumption"
  )(h => (h.householdId, h.stdOrToU, h.energyReadingTime, h.energyConsumption))

  implicit val householdEnergyConsumptionDecoder: Decoder[HouseholdEnergyConsumption] = Decoder.forProduct4(
    "householdId",
    "stdOrToU",
    "energyReadingTime",
    "energyConsumption"
  )(HouseholdEnergyConsumption.apply)

  def apply(csvRecords: String): HouseholdEnergyConsumption = {
    val cols = csvRecords.split(",").map(_.trim)
    val householdId = cols(0)
    val stdOrToU = cols(1)
    val energyReadingTimestamp = Timestamp.valueOf(cols(2))
    val energyConsumption = cols(3) match {
      case "Null" => None
      case value => Some(value.toDouble)
    }
    HouseholdEnergyConsumption(householdId, stdOrToU, energyReadingTimestamp, energyConsumption)
  }

  def sortHouseholdEnergyData(householdReadings: Seq[HouseholdEnergyConsumption]): Seq[HouseholdEnergyConsumption] = {
    householdReadings.sortBy(reading => (reading.energyReadingTime, reading.householdId))
  }

  def addRealWorldIrregularities(householdReadings: Seq[HouseholdEnergyConsumption]): Seq[HouseholdEnergyConsumption] = {
    householdReadings.zipWithIndex.flatMap { case (householdEnergyConsumption, i) =>
      i % 10 match {
        case 7 =>
          val lateRecordTimestamp = Timestamp.from(householdEnergyConsumption.energyReadingTime.toInstant.minus(Duration.ofHours(2)))
          val lateRecord = householdEnergyConsumption.copy(energyReadingTime = lateRecordTimestamp)
          Seq(householdEnergyConsumption, lateRecord)

        case 9 => Seq.empty[HouseholdEnergyConsumption]
        case a if i > 0 && a == 0 => Seq(householdEnergyConsumption, householdEnergyConsumption)
        case _ => Seq(householdEnergyConsumption)
      }
    }
  }
}
