package dd.configurators

import dd.comparators.{AuthorsComparator, DiceComparator, ExactComparator, NGramComparator, RegexComparator}
import dd.finders.LuceneDocsFinder
import dd.interfaces.{Comparator, DocsFinder, Reporter}
import dd.reporters.PipeReporter
import play.api.libs.json.{JsArray, JsLookupResult, JsObject, JsValue, Json}

import java.io.{BufferedWriter, File}
import java.nio.charset.Charset
import java.nio.file.{Files, StandardOpenOption}
import scala.io.Source
import scala.util.{Try, Using}

/*
{
  "finder" : {
    "lucene" : {
      "index": ""
      "searchField": "",
    }
  }

  "comparators" : [
    "exact" : {
      "fieldName" : "",
      "normalize" : true
    },
    "dice" : {
      "fieldName" : "",
      "normalize" : true,
      "minSimilarity" : 80.5
    },
    "ngram" : {
      "fieldName" : "",
      "normalize" : true,
      "minSimilarity" : 80.5
    },
    "regex" : {
      "fieldName" : "",
      "normalize" : true,
      "regex" : "",
      "compString": ""
    },
    "authors" : {
      "fieldName" : ""
    }
  ],

  "reporters" : [
    "pipe" : {
      "file" : "",
      "encoding"; "",
      "recordSeparator" : "",
      "putHeader" : true,
      "minTrue" : 5
    }
  ]
}
*/

/**
 * Parses the external JSON configuration used by the application.
 *
 * This object transforms the raw configuration document into the concrete
 * finder, comparator, and reporter instances consumed by the runtime pipeline,
 * centralizing validation and instantiation logic in one place.
 */
object ConfMain:
  /**
   * Parses the application configuration.
   *
   * @param jsonFile configuration file to parse
   * @return configured finder, comparators, and reporters
   */
  def parseConfig(jsonFile: File): Try[(DocsFinder, Seq[Comparator], Seq[Reporter])] =
    Using(Source.fromFile(jsonFile)):
      _.getLines().mkString("\n")
    .map(parseConfig)

  /**
   * Parses the application configuration.
   *
   * @param jsonStr raw JSON content to parse
   * @return configured finder, comparators, and reporters
   */
  private def parseConfig(jsonStr: String): (DocsFinder, Seq[Comparator], Seq[Reporter]) =
    val json: JsValue = Json.parse(jsonStr)

    val lucene: JsLookupResult = json \ "finder" \ "lucene"
    val finder: DocsFinder = new LuceneDocsFinder(
      (lucene \ "index").asOpt[String].getOrElse(throw new IllegalArgumentException("Missing 'finder/lucene/index'")),
      (lucene \ "searchField").asOpt[String].getOrElse(throw new IllegalArgumentException("Missing 'finder/lucene/searchField'"))
    )

    val comparators: Seq[Comparator] =
      (json \ "comparators").asOpt[JsArray]
        .map(parseComparators(_, jsonStr))
        .getOrElse(throw new IllegalArgumentException(s"Missing valid comparators: $jsonStr"))

    val reporters: Seq[Reporter] =
      (json \ "reporters").asOpt[JsArray]
        .map(parseReporters(_, jsonStr))
        .getOrElse(throw new IllegalArgumentException(s"Missing valid reporters: $jsonStr"))

    (finder, comparators, reporters)

  /**
   * Parses the configured comparators.
   *
   * @param jarray JSON array containing the configured entries
   * @param jsonStr raw JSON content to parse
   * @return parsed comparator instances
   */
  private def parseComparators(jarray: JsArray,
                               jsonStr: String): Seq[Comparator] =
    jarray.as[Seq[JsObject]].map(_.value).map:
      map =>
        Try(parseComparator(map, jsonStr)).recover:
          case exception => throw new IllegalAccessException(s"Invalid comparator parameter: ${exception.getMessage}")
        .get

  /**
   * Parses the configured reporters.
   *
   * @param jarray JSON array containing the configured entries
   * @param jsonStr raw JSON content to parse
   * @return parsed reporter instances
   */
  private def parseReporters(jarray: JsArray,
                             jsonStr: String): Seq[Reporter] =
    jarray.as[Seq[JsObject]].map(_.value).map:
      map =>
        Try(parseReporter(map, jsonStr)).recover:
          case exception => throw new IllegalAccessException(s"Invalid reporter parameter: ${exception.getMessage}")
        .get

  /**
   * Parses a single comparator entry from the configuration map.
   *
   * @param map JSON map containing one comparator declaration
   * @param jsonStr raw JSON content used in error reporting
   * @return configured comparator instance
   */
  private def parseComparator(map: collection.Map[String, JsValue],
                              jsonStr: String): Comparator =
    if map.contains("exact") then parseExactComparator(map("exact").as[JsObject])
    else if map.contains("dice") then parseDiceComparator(map("dice").as[JsObject])
    else if map.contains("ngram") then parseNGramComparator(map("ngram").as[JsObject])
    else if map.contains("regex") then parseRegexComparator(map("regex").as[JsObject])
    else if map.contains("authors") then parseAuthorsComparator(map("authors").as[JsObject])
    else throw new IllegalArgumentException(s"Invalid comparator: $jsonStr")

  /**
   * Parses a single reporter entry from the configuration map.
   *
   * @param map JSON map containing one reporter declaration
   * @param jsonStr raw JSON content used in error reporting
   * @return configured reporter instance
   */
  private def parseReporter(map: collection.Map[String, JsValue],
                            jsonStr: String): Reporter =
    if map.contains("pipe") then parsePipeReporter(map("pipe").as[JsObject])
    else throw new IllegalArgumentException(s"Invalid reporter: $jsonStr")

  /**
   * Parses the exact comparator configuration.
   *
   * @param json JSON object containing the selected configuration block
   * @return configured exact comparator
   */
  private def parseExactComparator(json: JsObject): ExactComparator =
    val map: collection.Map[String, JsValue] = json.value
    new ExactComparator(map("fieldName").asOpt[String].getOrElse(""), map("normalize").as[Boolean])

  /**
   * Parses the Dice comparator configuration.
   *
   * @param json JSON object containing the selected configuration block
   * @return configured Dice comparator
   */
  private def parseDiceComparator(json: JsObject): DiceComparator =
    val map: collection.Map[String, JsValue] = json.value
    new DiceComparator(map("fieldName").asOpt[String].getOrElse(""), map("normalize").as[Boolean],
      map("minSimilarity").as[Double])

  /**
   * Parses the n-gram comparator configuration.
   *
   * @param json JSON object containing the selected configuration block
   * @return configured n-gram comparator
   */
  private def parseNGramComparator(json: JsObject): NGramComparator =
    val map: collection.Map[String, JsValue] = json.value
    new NGramComparator(map("fieldName").asOpt[String].getOrElse(""), map("normalize").as[Boolean],
      map("minSimilarity").as[Double])

  /**
   * Parses the regex comparator configuration.
   *
   * @param json JSON object containing the selected configuration block
   * @return configured regex comparator
   */
  private def parseRegexComparator(json: JsObject): RegexComparator =
    val map: collection.Map[String, JsValue] = json.value
    new RegexComparator(map("fieldName").asOpt[String].getOrElse(""), map("normalize").as[Boolean],
      map("regex").asOpt[String].getOrElse(""), map("compString").asOpt[String].getOrElse(""))

  /**
   * Parses the authors comparator configuration.
   *
   * @param json JSON object containing the selected configuration block
   * @return configured authors comparator
   */
  private def parseAuthorsComparator(json: JsObject): AuthorsComparator =
    val map: collection.Map[String, JsValue] = json.value
    new AuthorsComparator(map("fieldName").asOpt[String].getOrElse(""))

  /**
   * Parses the pipe reporter configuration.
   *
   * @param json JSON object containing the selected configuration block
   * @return configured pipe reporter
   */
  private def parsePipeReporter(json: JsObject): PipeReporter =
    val map: collection.Map[String, JsValue] = json.value
    val encoding: String = map("encoding").asOpt[String].orElse(Some("")).get
    val writer: BufferedWriter = Files.newBufferedWriter(new File(map("file").asOpt[String].orElse(Some("")).get).toPath,
      Charset.forName(encoding), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
    val minTrue: Int = map("minTrue").asOpt[Int].getOrElse(0)
    new PipeReporter(writer, map("recordSeparator").asOpt[String].orElse(Some("")).get, map("putHeader").as[Boolean], minTrue)
