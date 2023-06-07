name := """play-api"""
organization := "com.example"

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.13.10"

libraryDependencies ++= Seq( guice,
"org.scalatestplus.play" %% "scalatestplus-play" % "5.0.0" % Test,
  "com.typesafe.play" %% "play-json" % "2.9.2",
  "org.scalaj" %% "scalaj-http" % "2.4.2",
  "com.googlecode.libphonenumber" % "libphonenumber" % "8.12.53",
  "com.googlecode.libphonenumber" % "geocoder" % "2.189",
  "org.jsoup" % "jsoup" % "1.15.2",
  "com.github.vickumar1981" %% "stringdistance" % "1.2.7",
  "com.github.nscala-time" %% "nscala-time" % "2.30.0",
  "com.github.tototoshi" %% "scala-csv" % "1.3.10",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.3",
  "com.github.pathikrit" %% "better-files" % "3.9.1",
  //"net.zileo" % "logback-logdna" % "1.2.0",
  "com.typesafe.play" %% "play-logback" % "2.8.16",
  "com.typesafe.akka" %% "akka-actor" % "2.6.20",
  "com.typesafe.akka" %% "akka-stream" % "2.6.20",
  "com.typesafe.akka" %% "akka-slf4j" % "2.6.20",
  "com.typesafe.akka" %% "akka-testkit" % "2.6.20",
  "com.newmotion" %% "akka-rabbitmq" % "6.0.0",
  "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "3.0.4",
  "com.typesafe.akka" %% "akka-http" % "10.1.15",
  "com.typesafe.akka" %% "akka-http-xml" % "10.1.15",
  "com.typesafe.play" %% "play-ahc-ws-standalone" % "2.1.10",
  "com.typesafe.play" %% "play-ws-standalone-json" % "2.1.10",
  "com.amazonaws" % "aws-java-sdk" % "1.12.270",
  "net.openhft" % "chronicle-map" % "3.23ea3",
  "org.apache.poi" % "poi" % "5.2.2",
  "org.apache.poi" % "poi-ooxml" % "5.2.2",
  "net.debasishg" %% "redisclient" % "3.42",
  "net.openhft" % "chronicle-bytes" % "2.23ea24",
  "com.typesafe.play" %% "play-ws" % "2.8.18",
  "org.mongodb.scala" %% "mongo-scala-driver" % "4.9.0")

// Adds additional packages into Twirl
//TwirlKeys.templateImports += "com.example.controllers._"

// Adds additional packages into conf/routes
// play.sbt.routes.RoutesKeys.routesImport += "com.example.binders._"
