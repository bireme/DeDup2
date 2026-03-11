name := "DeDup2"

version := "0.1"

scalaVersion := "3.8.2"

val luceneVersion = "10.3.2"
val stringDistanceVersion = "1.2.7"
val commonsCVSVersion = "1.14.1"
val playJsonVersion = "3.1.0-M1"
val mysqlConnectorJVersion = "9.6.0"
val mongoVersion = "5.6.3"

libraryDependencies ++= Seq(
  "org.apache.lucene" % "lucene-analysis-common" % luceneVersion,
  "org.apache.lucene" % "lucene-core" % luceneVersion,
  "org.apache.lucene" % "lucene-queryparser" % luceneVersion,
  "org.apache.lucene" % "lucene-suggest" % luceneVersion,
  "org.apache.lucene" % "lucene-backward-codecs" % luceneVersion,
  "org.apache.commons" % "commons-csv" % commonsCVSVersion,
  "org.playframework" %% "play-json" % playJsonVersion,
  "com.mysql" % "mysql-connector-j" % mysqlConnectorJVersion,
  "org.mongodb" % "mongodb-driver-sync" % mongoVersion,
  "org.scalameta" %% "munit" % "1.2.1" % Test
)

/*assembly / assemblyMergeStrategy := {
  case "module-info.class" => MergeStrategy.first //MergeStrategy.discard
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}*/

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Wunused:all")
