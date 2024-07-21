# Energy stream

## Features

### Injest CSV into Kafka

Within `/energy-stream` is a Scala program that will injest csv data from [SmartMeter Energy Consumption Data in London Households](https://data.london.gov.uk/dataset/smartmeter-energy-use-data-in-london-households). You will have to download the tariff and either the full household data or partitioned household data file and place them within the root directory.

The `HouseholdReadingsIngestor` and `TariffIngestor` objects will take in 3 arguments -> 1. topic-name (the docker-compose will create `consumer-reading` and `tariff` topics on run) 2. bootstrap-server (localhost:29092 will be set up in docker-compose) 3. path to data (enter the full path to wherever you downloaded the files)

These objects will injest the csv and convert them to JSON to be sent to a producer.

With the docker-compose buld running -- Run:

```
sbt "run consumer-reading localhost:29092 {path to household data csv}"
```
