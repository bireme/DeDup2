package dd.configurators

import dd.comparators.{AuthorsComparator, DiceComparator, ExactComparator, NGramComparator, RegexComparator}
import dd.finders.LuceneDocsFinder
import dd.heuristics.{DirevHeuristic, LilacsMntHeuristic, LilacsMntamHeuristic, LilacsSasHeuristic, LilacsSasSourceHeuristic, LisHeuristic}
import dd.interfaces.{Comparator, DocsFinder, DocsProducer, Heuristics, Reporter}
import dd.producers.{CSVProducer, JsonProducer, MongoDBProducer, MongoDBProducerConfig, MySqlProducerConfig, MysqlProducer}
import dd.reporters.{JsonReporter, LuceneReporter, MongoDBReporter, PipeReporter}
import play.api.libs.json.{JsArray, JsLookupResult, JsObject, JsValue, Json}

import java.io.{BufferedWriter, File}
import java.nio.charset.Charset
import java.nio.file.{Files, StandardOpenOption}
import scala.io.Source
import scala.util.{Try, Using}

/**
 * Parses the external JSON configuration used by the application.
 *
 * This object transforms the raw configuration document into the concrete
 * finder, comparator, and reporter instances consumed by the runtime pipeline,
 * centralizing validation and instantiation logic in one place.
 */
object ConfMain:
  /**
   * Reporter instance paired with the extra fields it should include in output.
   *
   * @param reporter configured reporter implementation
   * @param otherFields additional field names emitted by this reporter
   */
  case class ConfiguredReporter(reporter: Reporter,
                                otherFields: Seq[String])

  /**
   * Fully parsed SimilarDocs runtime configuration.
   *
   * @param producer source producer that supplies input documents
   * @param finder finder used to retrieve candidate documents
   * @param comparators comparison filters applied to each document pair
   * @param reporters reporters notified for accepted comparison results
   * @param heuristic optional duplicate heuristic used to filter candidate documents
   * @param auxQuery optional auxiliary query passed to the finder
   * @param maxDocs optional maximum number of candidate documents per source document
   * @param documentParallelism maximum number of source documents processed concurrently
   */
  case class SimilarDocsConfig(producer: DocsProducer,
                               finder: DocsFinder,
                               comparators: Seq[Comparator],
                               reporters: Seq[ConfiguredReporter],
                               auxQuery: Option[String],
                               maxDocs: Option[Int],
                               documentParallelism: Int = Runtime.getRuntime.availableProcessors().max(1),
                               heuristic: Option[Heuristics] = None)

  /**
   * Parsed CSV producer settings that can be reused without instantiating a producer.
   *
   * @param csvFile CSV file path
   * @param schema positional schema mapping column positions to field names
   * @param hasHeader whether the CSV file contains a header row
   * @param fieldSeparator CSV field separator
   * @param encoding CSV file character encoding
   */
  case class CsvProducerConfig(csvFile: String,
                               schema: Map[Int, String],
                               hasHeader: Boolean,
                               fieldSeparator: Char,
                               encoding: String)

  /**
   * Marker type for supported SelfCheckDuplicated producer sources.
   */
  sealed trait SelfCheckSourceConfig

  /**
   * Self-check source backed by a MySQL query.
   *
   * @param mysql parsed MySQL producer configuration
   */
  case class SelfCheckMysqlSourceConfig(mysql: MySqlProducerConfig) extends SelfCheckSourceConfig

  /**
   * Self-check source backed directly by a CSV file.
   *
   * @param csv parsed CSV producer configuration
   */
  case class SelfCheckCsvSourceConfig(csv: CsvProducerConfig) extends SelfCheckSourceConfig

  /**
   * Fully parsed SelfCheckDuplicated runtime configuration.
   *
   * @param source source producer configuration used by the self-check workflow
   * @param searchField Lucene field used for candidate retrieval
   * @param minSimilarity minimum normalized n-gram similarity accepted by the Lucene finder
   * @param comparators comparison filters applied to each document pair
   * @param reporters reporters notified for accepted comparison results
   * @param heuristic optional duplicate heuristic used to filter candidate documents
   * @param auxQuery optional auxiliary query passed to the finder
   * @param maxDocs optional maximum number of candidate documents per source document
   * @param documentParallelism maximum number of source documents processed concurrently
   * @param csvEncoding encoding used for CSV generated from MySQL sources
   * @param outCsvFile optional CSV output path for MySQL sources
   * @param index optional Lucene index path for the generated self-check index
   */
  case class SelfCheckDuplicatedConfig(source: SelfCheckSourceConfig,
                                       searchField: String,
                                       minSimilarity: Double,
                                       comparators: Seq[Comparator],
                                       reporters: Seq[ConfiguredReporter],
                                       auxQuery: Option[String],
                                       maxDocs: Option[Int],
                                       documentParallelism: Int,
                                       csvEncoding: String,
                                       outCsvFile: Option[String],
                                       index: Option[String],
                                       heuristic: Option[Heuristics] = None)

  private val requiredReportFields: Seq[String] = Seq("dbase", "id")
  private val heuristicFactories: Map[String, () => Heuristics] = Map(
    "LilacsSasHeuristic" -> (() => new LilacsSasHeuristic),
    "dd.heuristics.LilacsSasHeuristic" -> (() => new LilacsSasHeuristic),
    "DirevHeuristic" -> (() => new DirevHeuristic),
    "dd.heuristics.DirevHeuristic" -> (() => new DirevHeuristic),
    "LisHeuristic" -> (() => new LisHeuristic),
    "dd.heuristics.LisHeuristic" -> (() => new LisHeuristic),
    "LilacsSasSourceHeuristic" -> (() => new LilacsSasSourceHeuristic),
    "dd.heuristics.LilacsSasSourceHeuristic" -> (() => new LilacsSasSourceHeuristic),
    "LilacsMntHeuristic" -> (() => new LilacsMntHeuristic),
    "dd.heuristics.LilacsMntHeuristic" -> (() => new LilacsMntHeuristic),
    "LilacsMntamHeuristic" -> (() => new LilacsMntamHeuristic),
    "dd.heuristics.LilacsMntamHeuristic" -> (() => new LilacsMntamHeuristic)
  )

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
   * Parses the complete SimilarDocs configuration, including the source
   * producer and runtime finder options.
   *
   * @param jsonFile configuration file to parse
   * @return configured source producer, finder, comparators, and reporters
   */
  def parseSimilarDocsConfig(jsonFile: File): Try[SimilarDocsConfig] =
    Using(Source.fromFile(jsonFile)):
      _.getLines().mkString("\n")
    .map(parseSimilarDocsConfig)

  /**
   * Parses the self-check configuration from the same JSON document accepted by
   * SimilarDocs. The producer may be either MySQL or CSV. MySQL input is first
   * exported to CSV; CSV input is indexed directly.
   *
   * @param jsonFile configuration file to parse
   * @return configured source, Lucene options, comparators, and reporters
   */
  def parseSelfCheckDuplicatedConfig(jsonFile: File): Try[SelfCheckDuplicatedConfig] =
    Using(Source.fromFile(jsonFile)):
      _.getLines().mkString("\n")
    .map(parseSelfCheckDuplicatedConfig)

  /**
   * Parses only the Lucene search field and comparators from the application
   * configuration.
   *
   * This is useful for workflows that create their own finder and reporters but
   * still want to reuse the comparator configuration syntax.
   *
   * @param jsonFile configuration file to parse
   * @return configured Lucene search field and comparators
   */
  def parseSimilarityConfig(jsonFile: File): Try[(String, Seq[Comparator])] =
    Using(Source.fromFile(jsonFile)):
      _.getLines().mkString("\n")
    .map(parseSimilarityConfig)

  /**
   * Parses the application configuration.
   *
   * @param jsonStr raw JSON content to parse
   * @return configured finder, comparators, and reporters
   */
  private def parseConfig(jsonStr: String): (DocsFinder, Seq[Comparator], Seq[Reporter]) =
    val json: JsValue = Json.parse(jsonStr)

    val finder: DocsFinder = parseFinder(json)

    val comparators: Seq[Comparator] =
      (json \ "comparators").asOpt[JsArray]
        .map(parseComparators(_, jsonStr))
        .getOrElse(throw new IllegalArgumentException(s"Missing valid comparators: $jsonStr"))

    val reporters: Seq[Reporter] =
      (json \ "reporters").asOpt[JsArray]
        .map(parseConfiguredReporters(_, jsonStr).map(_.reporter))
        .getOrElse(throw new IllegalArgumentException(s"Missing valid reporters: $jsonStr"))

    (finder, comparators, reporters)

  /**
   * Parses the complete SimilarDocs configuration from raw JSON.
   *
   * @param jsonStr raw JSON content to parse
   * @return configured source producer, finder, comparators, and reporters
   */
  private[dd] def parseSimilarDocsConfig(jsonStr: String): SimilarDocsConfig =
    val json: JsValue = Json.parse(jsonStr)

    val producer: DocsProducer =
      (json \ "producer").asOpt[JsObject]
        .map(parseProducer(_, jsonStr))
        .getOrElse(throw new IllegalArgumentException(s"Missing valid producer: $jsonStr"))

    val finder: DocsFinder = parseFinder(json)

    val comparators: Seq[Comparator] =
      (json \ "comparators").asOpt[JsArray]
        .map(parseComparators(_, jsonStr))
        .getOrElse(throw new IllegalArgumentException(s"Missing valid comparators: $jsonStr"))

    val reporters: Seq[ConfiguredReporter] =
      (json \ "reporters").asOpt[JsArray]
        .map(parseConfiguredReporters(_, jsonStr))
        .getOrElse(throw new IllegalArgumentException(s"Missing valid reporters: $jsonStr"))

    val heuristic: Option[Heuristics] = parseHeuristic(json)

    val lucene: JsLookupResult = json \ "finder" \ "lucene"
    SimilarDocsConfig(
      producer = producer,
      finder = finder,
      comparators = comparators,
      reporters = reporters,
      auxQuery = (lucene \ "auxQuery").asOpt[String].filter(_.nonEmpty),
      maxDocs = (lucene \ "maxDocs").asOpt[Int],
      documentParallelism = parseDocumentParallelism(json),
      heuristic = heuristic
    )

  /**
   * Parses the SelfCheckDuplicated configuration from raw JSON.
   *
   * @param jsonStr raw JSON content to parse
   * @return configured self-check workflow
   */
  private[dd] def parseSelfCheckDuplicatedConfig(jsonStr: String): SelfCheckDuplicatedConfig =
    val json: JsValue = Json.parse(jsonStr)

    val source: SelfCheckSourceConfig =
      (json \ "producer").asOpt[JsObject]
        .map(parseSelfCheckSource)
        .getOrElse(throw new IllegalArgumentException("SelfCheckDuplicated requires 'producer/mysql' or 'producer/csv'"))

    val lucene: JsLookupResult = json \ "finder" \ "lucene"
    val searchField: String =
      (lucene \ "searchField").asOpt[String].getOrElse(throw new IllegalArgumentException("Missing 'finder/lucene/searchField'"))
    val minSimilarity: Double = parseLuceneMinSimilarity(lucene)

    val comparators: Seq[Comparator] =
      (json \ "comparators").asOpt[JsArray]
        .map(parseComparators(_, jsonStr))
        .getOrElse(throw new IllegalArgumentException(s"Missing valid comparators: $jsonStr"))

    val reporters: Seq[ConfiguredReporter] =
      (json \ "reporters").asOpt[JsArray]
        .map(parseConfiguredReporters(_, jsonStr))
        .getOrElse(throw new IllegalArgumentException(s"Missing valid reporters: $jsonStr"))

    val heuristic: Option[Heuristics] = parseHeuristic(json)

    val selfCheck: JsLookupResult = json \ "selfCheckDuplicated"
    SelfCheckDuplicatedConfig(
      source = source,
      searchField = searchField,
      minSimilarity = minSimilarity,
      comparators = comparators,
      reporters = reporters,
      auxQuery = (lucene \ "auxQuery").asOpt[String].filter(_.nonEmpty),
      maxDocs = (lucene \ "maxDocs").asOpt[Int],
      documentParallelism = parseDocumentParallelism(json),
      csvEncoding = (selfCheck \ "encoding").asOpt[String].filter(_.nonEmpty).getOrElse("utf-8"),
      outCsvFile = (selfCheck \ "outCsvFile").asOpt[String].filter(_.nonEmpty),
      index = (selfCheck \ "index").asOpt[String].filter(_.nonEmpty),
      heuristic = heuristic
    )

  /**
   * Resolves the optional heuristic name configured at the document root.
   *
   * @param json raw application configuration
   * @return the configured heuristic, or {@code None} when no name is present
   */
  private def parseHeuristic(json: JsValue): Option[Heuristics] =
    (json \ "heuristic").asOpt[String].map(_.trim).filter(_.nonEmpty).map:
      name =>
        heuristicFactories.get(name) match
          case Some(factory) => factory()
          case None => throw new IllegalArgumentException(s"Unknown heuristic: $name")

  /**
   * Parses the Lucene search field and comparator definitions from raw JSON.
   *
   * @param jsonStr raw JSON content to parse
   * @return configured Lucene search field and comparators
   */
  private[dd] def parseSimilarityConfig(jsonStr: String): (String, Seq[Comparator]) =
    val json: JsValue = Json.parse(jsonStr)

    val lucene: JsLookupResult = json \ "finder" \ "lucene"
    val searchField: String =
      (lucene \ "searchField").asOpt[String].getOrElse(throw new IllegalArgumentException("Missing 'finder/lucene/searchField'"))
    parseLuceneMinSimilarity(lucene)

    val comparators: Seq[Comparator] =
      (json \ "comparators").asOpt[JsArray]
        .map(parseComparators(_, jsonStr))
        .getOrElse(throw new IllegalArgumentException(s"Missing valid comparators: $jsonStr"))

    (searchField, comparators)

  /**
   * Parses the configured source producer.
   *
   * @param json JSON object containing one producer declaration
   * @param jsonStr raw JSON content used in error reporting
   * @return configured source producer
   */
  private def parseProducer(json: JsObject,
                            jsonStr: String): DocsProducer =
    val map: collection.Map[String, JsValue] = json.value
    if map.contains("csv") then parseCSVProducer(map("csv").as[JsObject])
    else if map.contains("json") then parseJsonProducer(map("json").as[JsObject])
    else if map.contains("mysql") then parseMysqlProducer(map("mysql").as[JsObject])
    else if map.contains("mongoDB") then parseMongoDBProducer(map("mongoDB").as[JsObject])
    else if map.contains("mongodb") then parseMongoDBProducer(map("mongodb").as[JsObject])
    else throw new IllegalArgumentException(s"Invalid producer: $jsonStr")

  /**
   * Parses the CSV source producer configuration.
   *
   * @param json JSON object containing the CSV producer configuration
   * @return configured CSV producer
   */
  private def parseCSVProducer(json: JsObject): CSVProducer =
    val config: CsvProducerConfig = parseCSVProducerConfig(json)
    new CSVProducer(
      csv = config.csvFile,
      schema = config.schema,
      hasHeader = config.hasHeader,
      fieldSeparator = config.fieldSeparator,
      csvFileEncoding = config.encoding
    )

  /**
   * Parses CSV producer settings without opening the CSV file.
   *
   * This is used by SelfCheckDuplicated so it can reuse the configured CSV
   * file and schema while building its temporary Lucene index.
   *
   * @param json JSON object containing the CSV producer configuration
   * @return parsed CSV producer configuration
   */
  private def parseCSVProducerConfig(json: JsObject): CsvProducerConfig =
    val map: collection.Map[String, JsValue] = json.value
    val schemaContent: String = readSchema(requiredString(map, "schema").trim)
    val schema: Map[Int, String] = parseSchema(schemaContent)
    requireSchemaFields(schema)

    CsvProducerConfig(
      csvFile = requiredString(map, "file"),
      schema = schema,
      hasHeader = map.get("hasHeader").flatMap(_.asOpt[Boolean]).getOrElse(false),
      fieldSeparator = optionalString(map, "fieldSeparator")
        .orElse(optionalString(map, "fieldSep"))
        .flatMap(_.headOption)
        .getOrElse(','),
      encoding = optionalString(map, "encoding").getOrElse("utf-8")
    )

  /**
   * Parses the JSON source producer configuration.
   *
   * @param json JSON object containing the JSON producer configuration
   * @return configured JSON producer
   */
  private def parseJsonProducer(json: JsObject): JsonProducer =
    val map: collection.Map[String, JsValue] = json.value
    new JsonProducer(
      input = optionalString(map, "input")
        .orElse(optionalString(map, "file"))
        .orElse(optionalString(map, "json"))
        .getOrElse(throw new IllegalArgumentException("Missing 'input'")),
      fields = optionalStringSeq(map, "fields"),
      encoding = optionalString(map, "encoding").getOrElse("utf-8")
    )

  /**
   * Parses the MySQL source producer configuration.
   *
   * @param json JSON object containing the MySQL producer configuration
   * @return configured MySQL producer
   */
  private def parseMysqlProducer(json: JsObject): MysqlProducer =
    new MysqlProducer(parseMysqlProducerConfig(json))

  /**
   * Parses and instantiates a MongoDB document producer.
   *
   * @param json JSON object containing the MongoDB producer configuration
   * @return configured MongoDB producer
   */
  private def parseMongoDBProducer(json: JsObject): MongoDBProducer =
    val map: collection.Map[String, JsValue] = json.value
    new MongoDBProducer(
      MongoDBProducerConfig(
        database = requiredString(map, "database"),
        collection = requiredString(map, "collection"),
        query = optionalString(map, "query"),
        projection = optionalString(map, "projection"),
        host = optionalString(map, "host"),
        port = optionalInt(map, "port"),
        user = optionalString(map, "user"),
        password = optionalString(map, "password"),
        fields = optionalStringSeq(map, "fields"),
        noCursorTimeout = map.get("noCursorTimeout").flatMap(_.asOpt[Boolean]).getOrElse(true)
      )
    )

  /**
   * Parses MySQL producer settings without opening a database connection.
   *
   * This is used by SimilarDocs and SelfCheckDuplicated to centralize the
   * JSON-to-configuration conversion for MySQL-backed sources. The optional
   * `splitDocumentField` property names a JSON-array field whose occurrences
   * are emitted as separate documents instead of one `//@//`-joined value.
   *
   * @param json JSON object containing the MySQL producer configuration
   * @return parsed MySQL producer configuration
   */
  private def parseMysqlProducerConfig(json: JsObject): MySqlProducerConfig =
    val map: collection.Map[String, JsValue] = json.value
    MySqlProducerConfig(
      mySqlHost = optionalString(map, "mySqlHost").orElse(optionalString(map, "host")).getOrElse(throw new IllegalArgumentException("Missing 'mySqlHost'")),
      mySqlPort = optionalInt(map, "mySqlPort").orElse(optionalInt(map, "port")).getOrElse(3306),
      mySqlDbname = optionalString(map, "mySqlDbname").orElse(optionalString(map, "dbnm")).getOrElse(throw new IllegalArgumentException("Missing 'mySqlDbname'")),
      mySqlUser = optionalString(map, "mySqlUser").orElse(optionalString(map, "user")).getOrElse(throw new IllegalArgumentException("Missing 'mySqlUser'")),
      mySqlPassword = optionalString(map, "mySqlPassword").orElse(optionalString(map, "pswd")).getOrElse(throw new IllegalArgumentException("Missing 'mySqlPassword'")),
      sqlfs = optionalStringSeq(map, "sqlfs")
        .orElse(optionalStringSeq(map, "sqlf"))
        .orElse(optionalStringSeq(map, "sqls"))
        .getOrElse(throw new IllegalArgumentException("Missing 'sqlfs'")),
      sqlEncoding = optionalString(map, "sqlEncoding").getOrElse("utf-8"),
      jsonFields = parseJsonFields(map),
      repetitiveFields = optionalStringSeq(map, "repetitiveFields")
        .orElse(optionalStringSeq(map, "repetitiveField"))
        .map(_.toSet),
      repetitiveSep = optionalString(map, "repetitiveSep"),
      splitDocumentField = optionalString(map, "splitDocumentField")
    )

  /**
   * Parses the producer source supported by SelfCheckDuplicated.
   *
   * Self-check accepts MySQL and CSV sources. MongoDB is intentionally excluded
   * because the workflow currently prepares a CSV source before creating the
   * temporary Lucene index.
   *
   * @param json JSON object containing a supported producer declaration
   * @return parsed self-check source configuration
   */
  private def parseSelfCheckSource(json: JsObject): SelfCheckSourceConfig =
    val map: collection.Map[String, JsValue] = json.value
    if map.contains("mysql") then SelfCheckMysqlSourceConfig(parseMysqlProducerConfig(map("mysql").as[JsObject]))
    else if map.contains("csv") then SelfCheckCsvSourceConfig(parseCSVProducerConfig(map("csv").as[JsObject]))
    else throw new IllegalArgumentException("SelfCheckDuplicated requires 'producer/mysql' or 'producer/csv'")

  /**
   * Parses the configured finder.
   *
   * @param json parsed root configuration
   * @return configured finder
   */
  private def parseFinder(json: JsValue): DocsFinder =
    val lucene: JsLookupResult = json \ "finder" \ "lucene"
    new LuceneDocsFinder(
      (lucene \ "index").asOpt[String].getOrElse(throw new IllegalArgumentException("Missing 'finder/lucene/index'")),
      (lucene \ "searchField").asOpt[String].getOrElse(throw new IllegalArgumentException("Missing 'finder/lucene/searchField'")),
      parseLuceneMinSimilarity(lucene)
    )

  private def parseLuceneMinSimilarity(lucene: JsLookupResult): Double =
    val value: Double = (lucene \ "minSimilarity").asOpt[Double]
      .getOrElse(throw new IllegalArgumentException("Missing 'finder/lucene/minSimilarity'"))
    if value < 0.0 || value > 1.0 then
      throw new IllegalArgumentException("'finder/lucene/minSimilarity' must be between 0.0 and 1.0")
    value

  private def parseDocumentParallelism(json: JsValue): Int =
    val default = Runtime.getRuntime.availableProcessors().max(1)
    val value = (json \ "documentParallelism").asOpt[Int]
      .orElse((json \ "parallelism").asOpt[Int])
      .getOrElse(default)
    if value <= 0 then throw new IllegalArgumentException("'documentParallelism' must be greater than zero")
    value

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
        Try(parseComparator(map, jsonStr)).fold(
          exception => throw new IllegalArgumentException(s"Invalid comparator parameter: ${exception.getMessage}", exception),
          identity
        )

  /**
   * Parses the configured reporters.
   * @param jarray JSON array containing the configured entries
   * @param jsonStr raw JSON content to parse
   * @return parsed reporter instances
   */
  private def parseConfiguredReporters(jarray: JsArray,
                                       jsonStr: String): Seq[ConfiguredReporter] =
    jarray.as[Seq[JsObject]].map(_.value).map:
      map =>
        Try(parseConfiguredReporter(map, jsonStr)).fold(
          exception => throw new IllegalArgumentException(s"Invalid reporter parameter: ${exception.getMessage}", exception),
          identity
        )

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
  private def parseConfiguredReporter(map: collection.Map[String, JsValue],
                                      jsonStr: String): ConfiguredReporter =
    if map.contains("pipe") then
      val json = map("pipe").as[JsObject]
      ConfiguredReporter(parsePipeReporter(json), parseOtherFields(json))
    else if map.contains("json") then
      val json = map("json").as[JsObject]
      ConfiguredReporter(parseJsonReporter(json), parseOtherFields(json))
    else if map.contains("mongoDB") then
      val json = map("mongoDB").as[JsObject]
      ConfiguredReporter(parseMongoDBReporter(json), parseOtherFields(json))
    else if map.contains("mongodb") then
      val json = map("mongodb").as[JsObject]
      ConfiguredReporter(parseMongoDBReporter(json), parseOtherFields(json))
    else if map.contains("lucene") then
      val json = map("lucene").as[JsObject]
      ConfiguredReporter(parseLuceneReporter(json), parseOtherFields(json))
    else throw new IllegalArgumentException(s"Invalid reporter: $jsonStr")

  /**
   * Parses the exact comparator configuration.
   *
   * @param json JSON object containing the selected configuration block
   * @return configured exact comparator
   */
  private def parseExactComparator(json: JsObject): ExactComparator =
    val map: collection.Map[String, JsValue] = json.value
    new ExactComparator(requiredString(map, "fieldName"), requiredBoolean(map, "normalize"))

  /**
   * Parses the Dice comparator configuration.
   *
   * @param json JSON object containing the selected configuration block
   * @return configured Dice comparator
   */
  private def parseDiceComparator(json: JsObject): DiceComparator =
    val map: collection.Map[String, JsValue] = json.value
    new DiceComparator(requiredString(map, "fieldName"), requiredBoolean(map, "normalize"),
      requiredDouble(map, "minSimilarity"))

  /**
   * Parses the n-gram comparator configuration.
   *
   * @param json JSON object containing the selected configuration block
   * @return configured n-gram comparator
   */
  private def parseNGramComparator(json: JsObject): NGramComparator =
    val map: collection.Map[String, JsValue] = json.value
    new NGramComparator(requiredString(map, "fieldName"), requiredBoolean(map, "normalize"),
      requiredDouble(map, "minSimilarity"))

  /**
   * Parses the regex comparator configuration.
   *
   * @param json JSON object containing the selected configuration block
   * @return configured regex comparator
   */
  private def parseRegexComparator(json: JsObject): RegexComparator =
    val map: collection.Map[String, JsValue] = json.value
    new RegexComparator(requiredString(map, "fieldName"), requiredBoolean(map, "normalize"),
      requiredString(map, "regex"), requiredString(map, "compString", allowEmpty = true))

  /**
   * Parses the authors comparator configuration.
   *
   * @param json JSON object containing the selected configuration block
   * @return configured authors comparator
   */
  private def parseAuthorsComparator(json: JsObject): AuthorsComparator =
    val map: collection.Map[String, JsValue] = json.value
    new AuthorsComparator(requiredString(map, "fieldName"))

  /**
   * Parses the pipe reporter configuration.
   *
   * @param json JSON object containing the selected configuration block
   * @return configured pipe reporter
   */
  private def parsePipeReporter(json: JsObject): PipeReporter =
    val map: collection.Map[String, JsValue] = json.value
    val encoding: String = requiredString(map, "encoding")
    val writer: BufferedWriter = Files.newBufferedWriter(prepareOutputFile(requiredString(map, "file")),
      Charset.forName(encoding), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
    val minTrue: Int = map.get("minTrue").flatMap(_.asOpt[Int]).getOrElse(0)
    val flushResults: Boolean = map.get("flushResults").flatMap(_.asOpt[Boolean]).getOrElse(false)
    new PipeReporter(writer, pipeRecordSeparator(map), requiredBoolean(map, "putHeader"), minTrue, flushResults)

  /**
   * Parses the JSON reporter configuration.
   *
   * @param json JSON object containing the selected configuration block
   * @return configured JSON reporter
   */
  private def parseJsonReporter(json: JsObject): JsonReporter =
    val map: collection.Map[String, JsValue] = json.value
    val encoding: String = requiredString(map, "encoding")
    val writer: BufferedWriter = Files.newBufferedWriter(prepareOutputFile(requiredString(map, "file")),
      Charset.forName(encoding), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
    val minTrue: Int = map.get("minTrue").flatMap(_.asOpt[Int]).getOrElse(0)
    val flushResults: Boolean = map.get("flushResults").flatMap(_.asOpt[Boolean]).getOrElse(false)
    new JsonReporter(writer, minTrue, flushResults)

  private def prepareOutputFile(fileName: String): java.nio.file.Path =
    val path = new File(fileName).toPath
    Option(path.getParent).foreach(parent => Files.createDirectories(parent))
    path

  /**
   * Parses the MongoDB reporter configuration.
   *
   * @param json JSON object containing the selected configuration block
   * @return configured MongoDB reporter
   */
  private def parseMongoDBReporter(json: JsObject): MongoDBReporter =
    val map: collection.Map[String, JsValue] = json.value
    new MongoDBReporter(
      database = requiredString(map, "database"),
      collection = requiredString(map, "collection"),
      append = requiredBoolean(map, "append"),
      host = map.get("host").flatMap(_.asOpt[String]),
      port = map.get("port").flatMap(_.asOpt[Int]),
      user = map.get("user").flatMap(_.asOpt[String]),
      password = map.get("password").flatMap(_.asOpt[String]),
      minTrue = map.get("minTrue").flatMap(_.asOpt[Int]).getOrElse(0),
      flushResults = map.get("flushResults").flatMap(_.asOpt[Boolean]).getOrElse(false)
    )

  /**
   * Parses the Lucene reporter configuration.
   *
   * @param json JSON object containing the selected configuration block
   * @return configured Lucene reporter
   */
  private def parseLuceneReporter(json: JsObject): LuceneReporter =
    val map: collection.Map[String, JsValue] = json.value
    new LuceneReporter(
      index = requiredString(map, "index"),
      fieldToIndex = requiredString(map, "fieldToIndex"),
      append = map.get("append").flatMap(_.asOpt[Boolean]).getOrElse(false),
      fieldNameMapping = parseFieldNameMapping(map),
      minTrue = map.get("minTrue").flatMap(_.asOpt[Int]).getOrElse(0)
    )

  /**
   * Reads a required string field from a JSON object map.
   *
   * @param map JSON object fields
   * @param key field name to read
   * @param allowEmpty whether the empty string is accepted as a valid value
   * @return parsed string value
   */
  private def requiredString(map: collection.Map[String, JsValue],
                             key: String,
                             allowEmpty: Boolean = false): String =
    map.get(key).flatMap(_.asOpt[String]).filter(value => allowEmpty || value.nonEmpty)
      .getOrElse(throw new IllegalArgumentException(s"Missing '$key'"))

  /**
   * Reads an optional string field from a JSON object map.
   *
   * @param map JSON object fields
   * @param key field name to read
   * @return parsed string value when present and non-empty
   */
  private def optionalString(map: collection.Map[String, JsValue],
                             key: String): Option[String] =
    map.get(key).flatMap(_.asOpt[String]).filter(_.nonEmpty)

  /**
   * Reads an optional integer field from a JSON object map.
   *
   * @param map JSON object fields
   * @param key field name to read
   * @return parsed integer value when present
   */
  private def optionalInt(map: collection.Map[String, JsValue],
                          key: String): Option[Int] =
    map.get(key).flatMap(_.asOpt[Int])

  /**
   * Reads an optional string list from a JSON object map.
   *
   * @param map JSON object fields
   * @param key field name to read
   * @return parsed list when present
   */
  private def optionalStringSeq(map: collection.Map[String, JsValue],
                                key: String): Option[Seq[String]] =
    map.get(key).flatMap:
      value =>
        value.asOpt[Seq[String]]
          .orElse(value.asOpt[String].map(splitCommaSeparated))
    .map(_.map(_.trim).filter(_.nonEmpty))
    .filter(_.nonEmpty)

  /**
   * Reads an optional string-to-string map from a JSON object map.
   *
   * @param map JSON object fields
   * @param key field name to read
   * @return parsed mapping when present
   */
  private def optionalStringMap(map: collection.Map[String, JsValue],
                                key: String): Option[Map[String, String]] =
    map.get(key).flatMap(_.asOpt[JsObject]).map:
      json =>
        json.value.map:
          case (from, to) => from -> to.as[String]
        .toMap

  /**
   * Parses optional field-name replacement settings for Lucene reporter output.
   *
   * @param map reporter configuration fields
   * @return field-name mapping, or an empty map when absent
   */
  private def parseFieldNameMapping(map: collection.Map[String, JsValue]): Map[String, String] =
    optionalStringMap(map, "fieldNameMapping")
      .orElse(optionalStringMap(map, "fieldMapping"))
      .orElse(optionalStringMap(map, "fieldNames"))
      .getOrElse(Map.empty)

  /**
   * Reads a required boolean field from a JSON object map.
   *
   * @param map JSON object fields
   * @param key field name to read
   * @return parsed boolean value
   */
  private def requiredBoolean(map: collection.Map[String, JsValue],
                              key: String): Boolean =
    map.get(key).flatMap(_.asOpt[Boolean])
      .getOrElse(throw new IllegalArgumentException(s"Missing '$key'"))

  /**
   * Reads a required numeric field from a JSON object map.
   *
   * @param map JSON object fields
   * @param key field name to read
   * @return parsed double value
   */
  private def requiredDouble(map: collection.Map[String, JsValue],
                             key: String): Double =
    map.get(key).flatMap(_.asOpt[Double])
      .getOrElse(throw new IllegalArgumentException(s"Missing '$key'"))

  /**
   * Resolves a valid pipe record separator from reporter configuration.
   *
   * @param map JSON object fields
   * @return configured record separator, defaulting invalid values to newline
   */
  private def pipeRecordSeparator(map: collection.Map[String, JsValue]): String =
    map.get("recordSeparator").flatMap(_.asOpt[String]) match
      case Some(separator) if separator.nonEmpty && separator != "|" => separator
      case _ => "\n"

  /**
   * Prepends required report fields while preserving configured fields.
   *
   * @param otherFields additional fields requested by a reporter
   * @return report fields including required identifiers without duplicates
   */
  private[dd] def includeRequiredReportFields(otherFields: Seq[String]): Seq[String] =
    (requiredReportFields ++ otherFields).foldLeft(Vector.empty[String]):
      case (acc, field) if acc.contains(field) => acc
      case (acc, field) => acc :+ field

  /**
   * Parses reporter-specific extra output fields.
   *
   * @param json reporter configuration
   * @return configured output fields including required identifiers
   */
  private def parseOtherFields(json: JsObject): Seq[String] =
    includeRequiredReportFields(optionalStringSeq(json.value, "otherFields").getOrElse(Seq.empty))

  /**
   * Loads a schema definition from inline text or an external file.
   *
   * @param schema raw schema configuration
   * @return schema text ready to parse
   */
  private def readSchema(schema: String): String =
    if schema.startsWith("file=") then
      Using(Source.fromFile(schema.substring(5)))(_.mkString.trim).get
    else schema

  /**
   * Parses a positional schema into a map.
   *
   * @param rawSchema raw schema content
   * @return schema mapping column positions to field names
   */
  private def parseSchema(rawSchema: String): Map[Int, String] =
    rawSchema.split(" *[,\n] *").map(_.trim).iterator.filter(_.nonEmpty).map(_.trim).map:
      _.split(" *= *", 2)
    .map:
      case Array(index, field) => index.trim.toInt -> field.trim
      case other => throw IllegalArgumentException(s"Invalid schema entry: ${other.mkString(":")}")
    .toMap

  /**
   * Validates whether a schema contains the fields required by reporters.
   *
   * @param schema input schema
   */
  private def requireSchemaFields(schema: Map[Int, String]): Unit =
    val schemaFields = schema.values.toSet
    val missing = requiredReportFields.filterNot(schemaFields.contains)
    if missing.nonEmpty then
      throw IllegalArgumentException(s"Schema missing required field(s): ${missing.mkString(", ")}")

  /**
   * Parses JSON field expansion settings for MySQL producers.
   *
   * @param map producer configuration fields
   * @return optional JSON field mapping
   */
  private def parseJsonFields(map: collection.Map[String, JsValue]): Option[Map[String, Map[String, String]]] =
    map.get("jsonFields").flatMap(_.asOpt[JsObject]).map(parseJsonFieldsObject)
      .orElse(optionalString(map, "jsonFieldFile").map(parseJsonFieldFile))

  /**
   * Parses an inline JSON field mapping object.
   *
   * @param json mapping object
   * @return JSON field mapping
   */
  private def parseJsonFieldsObject(json: JsObject): Map[String, Map[String, String]] =
    json.value.map:
      case (column, value) =>
        val mappings = value.as[JsObject].value.map:
          case (from, to) => from -> to.as[String]
        .toMap
        column -> mappings
    .toMap

  /**
   * Parses a JSON field mapping file.
   *
   * @param fileName mapping file path
   * @return JSON field mapping
   */
  private def parseJsonFieldFile(fileName: String): Map[String, Map[String, String]] =
    Using(Source.fromFile(fileName)):
      _.getLines().zipWithIndex.foldLeft(Map.empty[String, Map[String, String]]):
        case (map, (line, index)) =>
          parseJsonFieldMappingLine(fileName, line, index + 1) match
            case Some((column, from, to)) =>
              val mappings = map.getOrElse(column, Map.empty) + (from -> to)
              map + (column -> mappings)
            case None => map
    .get

  private def parseJsonFieldMappingLine(fileName: String,
                                        line: String,
                                        lineNumber: Int): Option[(String, String, String)] =
    Try(MysqlProducer.parseJsonFieldMappingLine(line)).recover:
      case exception: IllegalArgumentException =>
        throw IllegalArgumentException(
          s"Invalid jsonFieldFile entry at $fileName:$lineNumber. Expected '<column>=<json field>[-><output field>]', got: $line",
          exception
        )
    .get

  /**
   * Splits a comma-separated string value.
   *
   * @param value raw comma-separated value
   * @return parsed values
   */
  private def splitCommaSeparated(value: String): Seq[String] =
    value.split(" *, *").map(_.trim).filter(_.nonEmpty).toSeq
