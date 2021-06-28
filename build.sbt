name := "SmartAdIntegration"

version := "0.1"

scalaVersion := "2.13.3"

idePackagePrefix := Some("com.imvu.smartad")

val AkkaVersion = "2.6.15"
val AkkaHttpVersion = "10.2.4"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
  "com.typesafe.akka" %% "akka-actor" % AkkaVersion,
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
  "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % AkkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-xml" % AkkaHttpVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-file" % "3.0.1",
  "ch.qos.logback" % "logback-classic" % "1.2.3"
)