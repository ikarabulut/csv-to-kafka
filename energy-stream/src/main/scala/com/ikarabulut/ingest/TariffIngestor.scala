package com.ikarabulut.ingest

import com.ikarabulut.ingest.DataIngestor.ingestData
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.util.Using

object TariffIngestor {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    if (args.isEmpty || args.length != 3) {
      System.err.println("Usage: TariffIngestoor <topics> <bootstrap-servers> <tariff-data-path>")
      logger.error("Unable to start the TariffIngestoor as program arguments are missing.")
      sys.exit(1)
    }
    val Array(topic, bootstrapServers, tariffsPath) = args

    val rawTariffs = Using(Source.fromFile(tariffsPath))(_.getLines().drop(1).toSeq).getOrElse(Seq.empty)
    val tariffs = rawTariffs.map(Tariff(_))
    val tariffsJSON = tariffs.map(_.toJSONWithKey)
    ingestData(topic, bootstrapServers, tariffsJSON)
  }
}
