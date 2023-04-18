name := "DeDup2"

version := "0.1"

scalaVersion := "2.13.10"

val luceneVersion = "9.5.0" //"9.4.2"
val stringDistanceVersion = "1.2.7"
val commonsCVSVersion = "1.10.0" //"1.9.0"
val playJsonVersion = "2.10.0-RC7" //"2.10.0-RC6"

libraryDependencies ++= Seq(
  "org.apache.lucene" % "lucene-analysis-common" % luceneVersion,   // Lucene 9.0.0
  //"org.apache.lucene" % "lucene-analyzers-common" % luceneVersion,
  "org.apache.lucene" % "lucene-core" % luceneVersion,
  "org.apache.lucene" % "lucene-queryparser" % luceneVersion,
  "org.apache.lucene" % "lucene-suggest" % luceneVersion,
  "org.apache.lucene" % "lucene-backward-codecs" % luceneVersion,
  "com.github.vickumar1981" %% "stringdistance" % stringDistanceVersion,
  "org.apache.commons" % "commons-csv" % commonsCVSVersion,
  "com.typesafe.play" %% "play-json" % playJsonVersion
)

assembly / assemblyMergeStrategy := {
  case "module-info.class" => MergeStrategy.first //MergeStrategy.discard
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Ywarn-unused")
