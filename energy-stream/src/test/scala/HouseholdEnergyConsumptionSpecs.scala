package com.ikarabulut.ingest

import com.ikarabulut.ingest.HouseholdEnergyConsumption.{addRealWorldIrregularities, sortHouseholdEnergyData}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

class HouseholdEnergyConsumptionSpecs extends AnyFlatSpec with Matchers {
  "HouseholdEnergyConsumption" should "be able to create valid object from a csv record" in {
    val householdId = "MAC000002"
    val stdOrToU = "Std"
    val energyReadingTime = "2023-03-20 12:00:00.0000000"
    val energyConsumption = 0.663
    val csvRecord = s"$householdId,$stdOrToU,$energyReadingTime,$energyConsumption"
    val householdEnergyConsumption = HouseholdEnergyConsumption(csvRecord)
    householdEnergyConsumption.householdId should be(householdId)
    householdEnergyConsumption.stdOrToU should be(stdOrToU)
    householdEnergyConsumption.energyReadingTime should be(Timestamp.valueOf(energyReadingTime))
    householdEnergyConsumption.energyConsumption should be(Some(energyConsumption))
  }

  it should "be able to handle invalid or Null readings" in {
    val householdId = "MAC000002"
    val stdOrToU = "Std"
    val energyReadingTime = "2023-03-20 12:00:00.0000000"
    val energyConsumption = "Null"
    val csvRecord = s"$householdId,$stdOrToU,$energyReadingTime,$energyConsumption"
    val householdEnergyConsumption = HouseholdEnergyConsumption(csvRecord)
    householdEnergyConsumption.householdId should be(householdId)
    householdEnergyConsumption.stdOrToU should be(stdOrToU)
    householdEnergyConsumption.energyReadingTime should be(Timestamp.valueOf(energyReadingTime))
    householdEnergyConsumption.energyConsumption should be(None)
  }

  it should "be able to sort the records by reading time and householdId" in {
    val data = Seq(
      "MAC000002,Std,2014-02-27 00:00:00.0000000, 1.1799999",
      "MAC000002,Std,2014-02-27 00:30:00.0000000, 1.152",
      "MAC000002,Std,2014-02-27 01:00:00.0000000, 0.981",
      "MAC000002,Std,2014-02-27 01:30:00.0000000, 0.355",
      "MAC000002,Std,2014-02-27 02:00:00.0000000, 0.198",
      "MAC000002,Std,2014-02-27 02:30:00.0000000, 0.141",
      "MAC000002,Std,2014-02-27 03:00:00.0000000, 0.098",
      "MAC000002,Std,2014-02-27 03:30:00.0000000, 0.117",
      "MAC000002,Std,2014-02-27 04:00:00.0000000, 0.105",
      "MAC000002,Std,2014-02-27 04:30:00.0000000, 0.106",
      "MAC000002,Std,2014-02-27 05:00:00.0000000, 0.111",
      "MAC000003,Std,2014-02-27 00:00:00.0000000, 0.093",
      "MAC000003,Std,2014-02-27 00:30:00.0000000, 1.308",
      "MAC000003,Std,2014-02-27 01:00:00.0000000, 1.374",
      "MAC000003,Std,2014-02-27 01:30:00.0000000, 0.062",
      "MAC000003,Std,2014-02-27 02:00:00.0000000, 0.425",
      "MAC000003,Std,2014-02-27 02:30:00.0000000, 0.101",
      "MAC000003,Std,2014-02-27 03:00:00.0000000, 0.158",
      "MAC000003,Std,2014-02-27 03:30:00.0000000, 0.198",
      "MAC000003,Std,2014-02-27 04:00:00.0000000, 0.075",
      "MAC000003,Std,2014-02-27 04:30:00.0000000, 0.318",
      "MAC000003,Std,2014-02-27 05:00:00.0000000, 0.058",
      "MAC000004,Std,2014-02-27 00:00:00.0000000, 0",
      "MAC000004,Std,2014-02-27 00:30:00.0000000, 0",
      "MAC000004,Std,2014-02-27 01:00:00.0000000, 0",
      "MAC000004,Std,2014-02-27 01:30:00.0000000, 0",
      "MAC000004,Std,2014-02-27 02:00:00.0000000, 0.152",
      "MAC000004,Std,2014-02-27 02:30:00.0000000, 0",
      "MAC000004,Std,2014-02-27 03:00:00.0000000, 0",
      "MAC000004,Std,2014-02-27 03:30:00.0000000, 0",
      "MAC000004,Std,2014-02-27 04:00:00.0000000, 0.227",
      "MAC000004,Std,2014-02-27 04:30:00.0000000, 0",
      "MAC000004,Std,2014-02-27 05:00:00.0000000, 0"
    ).map(HouseholdEnergyConsumption(_))
    val expected = Seq(
      "MAC000002,Std,2014-02-27 00:00:00.0000000, 1.1799999",
      "MAC000003,Std,2014-02-27 00:00:00.0000000, 0.093",
      "MAC000004,Std,2014-02-27 00:00:00.0000000, 0",
      "MAC000002,Std,2014-02-27 00:30:00.0000000, 1.152",
      "MAC000003,Std,2014-02-27 00:30:00.0000000, 1.308",
      "MAC000004,Std,2014-02-27 00:30:00.0000000, 0",
      "MAC000002,Std,2014-02-27 01:00:00.0000000, 0.981",
      "MAC000003,Std,2014-02-27 01:00:00.0000000, 1.374",
      "MAC000004,Std,2014-02-27 01:00:00.0000000, 0",
      "MAC000002,Std,2014-02-27 01:30:00.0000000, 0.355",
      "MAC000003,Std,2014-02-27 01:30:00.0000000, 0.062",
      "MAC000004,Std,2014-02-27 01:30:00.0000000, 0",
      "MAC000002,Std,2014-02-27 02:00:00.0000000, 0.198",
      "MAC000003,Std,2014-02-27 02:00:00.0000000, 0.425",
      "MAC000004,Std,2014-02-27 02:00:00.0000000, 0.152",
      "MAC000002,Std,2014-02-27 02:30:00.0000000, 0.141",
      "MAC000003,Std,2014-02-27 02:30:00.0000000, 0.101",
      "MAC000004,Std,2014-02-27 02:30:00.0000000, 0",
      "MAC000002,Std,2014-02-27 03:00:00.0000000, 0.098",
      "MAC000003,Std,2014-02-27 03:00:00.0000000, 0.158",
      "MAC000004,Std,2014-02-27 03:00:00.0000000, 0",
      "MAC000002,Std,2014-02-27 03:30:00.0000000, 0.117",
      "MAC000003,Std,2014-02-27 03:30:00.0000000, 0.198",
      "MAC000004,Std,2014-02-27 03:30:00.0000000, 0",
      "MAC000002,Std,2014-02-27 04:00:00.0000000, 0.105",
      "MAC000003,Std,2014-02-27 04:00:00.0000000, 0.075",
      "MAC000004,Std,2014-02-27 04:00:00.0000000, 0.227",
      "MAC000002,Std,2014-02-27 04:30:00.0000000, 0.106",
      "MAC000003,Std,2014-02-27 04:30:00.0000000, 0.318",
      "MAC000004,Std,2014-02-27 04:30:00.0000000, 0",
      "MAC000002,Std,2014-02-27 05:00:00.0000000, 0.111",
      "MAC000003,Std,2014-02-27 05:00:00.0000000, 0.058",
      "MAC000004,Std,2014-02-27 05:00:00.0000000, 0").map(HouseholdEnergyConsumption(_))
    val actual = sortHouseholdEnergyData(data)
    actual.length should be(expected.length)
    actual should be(expected)
  }

  it should "be able add real world irregularities in the data" in {
    val data = Seq(
      "MAC000002,Std,2014-02-27 00:00:00.0000000, 1.1799999",
      "MAC000003,Std,2014-02-27 00:00:00.0000000, 0.093",
      "MAC000004,Std,2014-02-27 00:00:00.0000000, 0",
      "MAC000002,Std,2014-02-27 00:30:00.0000000, 1.152",
      "MAC000003,Std,2014-02-27 00:30:00.0000000, 1.308",
      "MAC000004,Std,2014-02-27 00:30:00.0000000, 0",
      "MAC000002,Std,2014-02-27 01:00:00.0000000, 0.981",
      "MAC000003,Std,2014-02-27 01:00:00.0000000, 1.374",
      "MAC000004,Std,2014-02-27 01:00:00.0000000, 0",
      "MAC000002,Std,2014-02-27 01:30:00.0000000, 0.355",
      "MAC000003,Std,2014-02-27 01:30:00.0000000, 0.062",
      "MAC000004,Std,2014-02-27 01:30:00.0000000, 0",
      "MAC000002,Std,2014-02-27 02:00:00.0000000, 0.198",
      "MAC000003,Std,2014-02-27 02:00:00.0000000, 0.425",
      "MAC000004,Std,2014-02-27 02:00:00.0000000, 0.152",
      "MAC000002,Std,2014-02-27 02:30:00.0000000, 0.141",
      "MAC000003,Std,2014-02-27 02:30:00.0000000, 0.101",
      "MAC000004,Std,2014-02-27 02:30:00.0000000, 0",
      "MAC000002,Std,2014-02-27 03:00:00.0000000, 0.098",
      "MAC000003,Std,2014-02-27 03:00:00.0000000, 0.158",
      "MAC000004,Std,2014-02-27 03:00:00.0000000, 0",
      "MAC000002,Std,2014-02-27 03:30:00.0000000, 0.117",
      "MAC000003,Std,2014-02-27 03:30:00.0000000, 0.198",
      "MAC000004,Std,2014-02-27 03:30:00.0000000, 0",
      "MAC000002,Std,2014-02-27 04:00:00.0000000, 0.105",
      "MAC000003,Std,2014-02-27 04:00:00.0000000, 0.075",
      "MAC000004,Std,2014-02-27 04:00:00.0000000, 0.227",
      "MAC000002,Std,2014-02-27 04:30:00.0000000, 0.106",
      "MAC000003,Std,2014-02-27 04:30:00.0000000, 0.318",
      "MAC000004,Std,2014-02-27 04:30:00.0000000, 0",
      "MAC000002,Std,2014-02-27 05:00:00.0000000, 0.111",
      "MAC000003,Std,2014-02-27 05:00:00.0000000, 0.058",
      "MAC000004,Std,2014-02-27 05:00:00.0000000, 0").map(HouseholdEnergyConsumption(_))
    val expected = Seq(
      "MAC000002,Std,2014-02-27 00:00:00.0000000, 1.1799999",
      "MAC000003,Std,2014-02-27 00:00:00.0000000, 0.093",
      "MAC000004,Std,2014-02-27 00:00:00.0000000, 0",
      "MAC000002,Std,2014-02-27 00:30:00.0000000, 1.152",
      "MAC000003,Std,2014-02-27 00:30:00.0000000, 1.308",
      "MAC000004,Std,2014-02-27 00:30:00.0000000, 0",
      "MAC000002,Std,2014-02-27 01:00:00.0000000, 0.981",
      "MAC000003,Std,2014-02-27 01:00:00.0000000, 1.374",
      "MAC000003,Std,2014-02-26 23:00:00.0000000, 1.374",
      "MAC000004,Std,2014-02-27 01:00:00.0000000, 0",
      "MAC000003,Std,2014-02-27 01:30:00.0000000, 0.062",
      "MAC000003,Std,2014-02-27 01:30:00.0000000, 0.062",
      "MAC000004,Std,2014-02-27 01:30:00.0000000, 0",
      "MAC000002,Std,2014-02-27 02:00:00.0000000, 0.198",
      "MAC000003,Std,2014-02-27 02:00:00.0000000, 0.425",
      "MAC000004,Std,2014-02-27 02:00:00.0000000, 0.152",
      "MAC000002,Std,2014-02-27 02:30:00.0000000, 0.141",
      "MAC000003,Std,2014-02-27 02:30:00.0000000, 0.101",
      "MAC000004,Std,2014-02-27 02:30:00.0000000, 0",
      "MAC000004,Std,2014-02-27 00:30:00.0000000, 0",
      "MAC000002,Std,2014-02-27 03:00:00.0000000, 0.098",
      "MAC000004,Std,2014-02-27 03:00:00.0000000, 0",
      "MAC000004,Std,2014-02-27 03:00:00.0000000, 0",
      "MAC000002,Std,2014-02-27 03:30:00.0000000, 0.117",
      "MAC000003,Std,2014-02-27 03:30:00.0000000, 0.198",
      "MAC000004,Std,2014-02-27 03:30:00.0000000, 0",
      "MAC000002,Std,2014-02-27 04:00:00.0000000, 0.105",
      "MAC000003,Std,2014-02-27 04:00:00.0000000, 0.075",
      "MAC000004,Std,2014-02-27 04:00:00.0000000, 0.227",
      "MAC000002,Std,2014-02-27 04:30:00.0000000, 0.106",
      "MAC000002,Std,2014-02-27 02:30:00.0000000, 0.106",
      "MAC000003,Std,2014-02-27 04:30:00.0000000, 0.318",
      "MAC000002,Std,2014-02-27 05:00:00.0000000, 0.111",
      "MAC000002,Std,2014-02-27 05:00:00.0000000, 0.111",
      "MAC000003,Std,2014-02-27 05:00:00.0000000, 0.058",
      "MAC000004,Std,2014-02-27 05:00:00.0000000, 0").map(HouseholdEnergyConsumption(_))
    val actual = addRealWorldIrregularities(data)
    actual.length should be(expected.length)
    actual should be(expected)
  }
}
