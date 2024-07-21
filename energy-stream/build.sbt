val scala3Version = "3.4.2"

lazy val root = project
  .in(file("."))
  .settings(
    name := "energy-stream",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := scala3Version,
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % "2.8.0",
      "org.slf4j" % "slf4j-api" % "1.7.30",
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "org.xerial.snappy" % "snappy-java" % "1.1.10.1",
      "junit" % "junit" % "4.13.2" % Test,
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      "org.specs2" %% "specs2-core" % "5.5.3" % Test,
      "org.specs2" %% "specs2-junit" % "5.5.3" % Test
    )
  )
