package com.ikarabulut.ingest

import com.ikarabulut.ingest.Tariff.simpleDateFormat
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

class TariffSpecs extends AnyFlatSpec with Matchers {
  "Tariff" should "be able to create valid object from a csv record" in {
    val tariffDateTimeStr = "01/01/2013 00:00"
    val tariffType = "Normal"
    val tariffDateTime = Timestamp.from(simpleDateFormat.parse(tariffDateTimeStr).toInstant)
    val expected = Tariff(tariffDateTime, tariffType)
    val csvRecords = s"$tariffDateTimeStr,$tariffType"
    val actual = Tariff(csvRecords)
    actual.tariffDateTime should be (tariffDateTime)
    actual.tariff should be (tariffType)
  }

  it should "be able to convert into JSON string" in {
    val tariffDateTimeStr = "01/01/2013 00:00"
    val tariffType = "Normal"
    val csvRecords = s"$tariffDateTimeStr,$tariffType"
    val tariff = Tariff(csvRecords)
    val expected = s"""{"tariffDateTime":"2013-01-01T06:00:00Z","tariff":"$tariffType"}"""
    val actual = tariff.toJSONWithKey._2
    actual should be (expected)
  }
}
