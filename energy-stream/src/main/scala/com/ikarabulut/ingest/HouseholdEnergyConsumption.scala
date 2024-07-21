package com.ikarabulut.ingest

import java.sql.Timestamp
import java.time.Duration

case class HouseholdEnergyConsumption(householdId: String,
                                      stdOrToU: String,
                                      energyReadingTime: Timestamp,
                                      energyConsumption: Option[Double])

object HouseholdEnergyConsumption {
  def apply(csvRecords: String): HouseholdEnergyConsumption = {
    val cols = csvRecords.split(",").map(_.trim)
    val householdId = cols(0)
    val stdOrToU = cols(1)
    val energyReadingTime = Timestamp.valueOf(cols(2))
    val energyConsumption = cols(3).toDoubleOption
    HouseholdEnergyConsumption(householdId, stdOrToU, energyReadingTime, energyConsumption)
  }

  def sortHouseholdEnergyData(householdReadings: Seq[HouseholdEnergyConsumption]): Seq[HouseholdEnergyConsumption] = householdReadings.sortBy(reading => (reading.energyReadingTime, reading.householdId))


  def addRealWorldIrregularities(householdReadings: Seq[HouseholdEnergyConsumption]): Seq[HouseholdEnergyConsumption] = {
    householdReadings.zipWithIndex.flatMap { case (householdEnergyConsumption, i) =>
      i % 10 match {
        case a if a == 7 =>
          val lateRecordTimestamp = Timestamp.from(householdEnergyConsumption.energyReadingTime.toInstant.minus(Duration.ofHours(2)))
          val lateRecord = householdEnergyConsumption.copy(energyReadingTime = lateRecordTimestamp)
          Seq(householdEnergyConsumption, lateRecord)

        case a if a == 9 => Seq.empty[HouseholdEnergyConsumption]
        case a if i > 0 && a == 0 => Seq(householdEnergyConsumption, householdEnergyConsumption)
        case _ => Seq(householdEnergyConsumption)
      }
    }
  }
}
