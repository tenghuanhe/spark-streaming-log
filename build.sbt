name := "spark-log"

version := "1.0"

scalaVersion := "2.10.5"

resolvers ++= Seq(
  "spray repo" at "http://repo.spray.io/",
  "typesafe repo" at "http://repo.typesafe.com/typesafe/releases/"
)

libraryDependencies ++= {
  val sparkV = "1.6.0"
  val sprayV = "1.3.2"
  val playJsonV = "2.4.8"
  val liftJsonV = "2.5.1"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkV,
    "org.apache.spark" %% "spark-streaming" % sparkV,
    "io.spray" %% "spray-json" % sprayV,
    "com.typesafe.play" %% "play-json" % playJsonV,
    "net.liftweb" %% "lift-json" % liftJsonV
  )
}