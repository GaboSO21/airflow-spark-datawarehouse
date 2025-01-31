name := "SparkApp"

scalaVersion :="2.12.10"

version :="1.0"

val sparkVersion = "3.4.1"
val vegasVersion = "0.3.11"
val postgresVersion = "42.2.2"

resolvers ++= Seq(
    "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
    "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
    "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    // logging
    "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
    "org.apache.logging.log4j" % "log4j-core" % "2.4.1",
    // postgres for DB
    "org.postgresql" % "postgresql" % postgresVersion
)

