package com.ikarabulut.ingest

import com.ikarabulut.ingest.DataIngestor.ingestData
import com.ikarabulut.ingest.HouseholdEnergyConsumption.{addRealWorldIrregularities, sortHouseholdEnergyData}
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.util.Using

object HouseholdReadingsIngestor {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    if (args.isEmpty || args.length != 3) {
      System.err.println("Usage: HouseholdReadingsIngestoor <topics> <bootstrap-servers> <household-data-path>")
      logger.error("Unable to start the HouseholdReadingsIngestoor as program arguments are missing.")
      sys.exit(1)
    }
    val Array(topic, bootstrapServers, householdReadingsPath) = args

    val rawHouseholdReadings = Using(Source.fromFile(householdReadingsPath))(_.getLines().drop(1).toSeq).getOrElse(Seq.empty)
    val householdReadings = rawHouseholdReadings.map(HouseholdEnergyConsumption(_))
    val householdReadingsSorted = sortHouseholdEnergyData(householdReadings)
    val householdReadingsRealWorld = addRealWorldIrregularities(householdReadingsSorted)
    val householdReadingsJSON = householdReadingsRealWorld.map(_.toJSONWithKey)
    ingestData(topic, bootstrapServers, householdReadingsJSON)
  }
}
