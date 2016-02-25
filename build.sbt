organization := "com.productfoundry"

name := "akka-persistence-couchbase"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.7"

fork in Test := true

javaOptions in Test += "-Xmx512M"

scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-deprecation",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Xfuture"
)

parallelExecution in Test := false

resolvers ++= Seq(
  "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"
)

val akkaVer = "2.4.2"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-persistence" % akkaVer,
  "com.couchbase.client" % "java-client" % "2.2.2",
  "commons-codec" % "commons-codec" % "1.10",
  "com.typesafe.play" %% "play-json" % "2.4.3",
  "com.typesafe.akka" %% "akka-persistence-tck" % akkaVer % "test",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)
