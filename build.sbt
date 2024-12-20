name := "DeDup2"

version := "0.1"

scalaVersion := "2.13.15"  // "3.5.2" not possible because stringdistance

val luceneVersion = "10.0.0" //"9.7.0" //"9.5.0" //"9.4.2"
val stringDistanceVersion = "1.2.7"
val commonsCVSVersion = "1.12.0" //"1.9.0"
val playJsonVersion = "2.10.6" //"2.10.0-RC6"

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

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Ywarn-unused")
