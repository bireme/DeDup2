package dd.tools

import dd.NGAnalyzer
import dd.interfaces.{DocsProducer, Document}
import dd.producers.{MySqlProducerConfig, MysqlProducer}

import scala.io.Source
import scala.util.{Failure, Success, Try, Using}

/**
 * Command-line utility that converts SQL query results into a Lucene index.
 *
 * The tool follows the SQL connection and JSON-field handling used by
 * `SQL2Json`, and applies the Lucene indexing flow used by `CSV2Lucene`.
 */
object SQL2Lucene:
  /**
   * Command usage information shown when required parameters are missing.
   */
  private val usageMessage: String =
    """usage: SQL2Lucene <options>
      |options:
      |	-mySqlHost=<host>       MySQL server host address
      |	-mySqlUser=<str>        MySQL database user
      |	-mySqlPassword=<str>    MySQL database password
      |	-mySqlDbname=<str>      MySQL database name
      |	-sqlfs=<name1>[,...,<nameN>] Comma-separated SQL statement files to execute sequentially.
      |	                        Results from each file are appended to the same Lucene index during this run.
      |	-index=<path>           Path to the index to be created
      |	-fieldToIndex=<name>     Name of the field to be indexed
      |	[-mySqlPort=<int>]      MySQL server port. Default is 3306.
      |	[-importFields=(<fieldName>,...,<fieldName>|file=<path>)] Fields that will be written to the Lucene documents.
      |	                         If absent, all fields returned by the SQL statements will be written.
      |	[-jsonFieldFile=<path>] Path to a text file with JSON field mappings, one per line, using: <column name>=<json field name>[-><new field name>]
      |	                        For mapped SQL columns, object fields are extracted from the JSON content and emitted with the configured new field names.
      |	                        When <new field name> is omitted, the SQL column name is used as the output field name.
      |	                        Missing JSON fields are ignored. JSON array values are grouped with '//@//'; arrays of non-objects keep the SQL column name.
      |	[-sqlEncoding=<str>]    SQL file character encoding. Default is 'utf-8'
      |	[-repetitiveField=<name>[,<name>,...,<name>]] Fields split into multiple documents when repetitiveSep is found.
      |	[-repetitiveSep=<str>]  Separator used by repetitiveField. Default is '//@//'.
      |
      |aliases:
      |	-host, -port, -user, -pswd and -dbnm are accepted as aliases for the corresponding MySQL options.
      |	-sqls and -sqlf are accepted as aliases for -sqlfs.""".stripMargin

  /**
   * Entry point used when the utility is executed from the command line.
   *
   * @param args command-line arguments received by the utility
   * @return no value; this method delegates to the main workflow
   */
  def main(args: Array[String]): Unit =
    run(args) match
      case Success(_) => println("Indexing finished successfully!")
      case Failure(exception) => Console.err.println(s"Indexing SQL records failed: ${exception.getMessage}")

  /**
   * Executes the SQL-to-Lucene indexing workflow.
   *
   * @param args command-line arguments received by the utility
   * @return result of the complete indexing operation
   */
  private def run(args: Array[String]): Try[Unit] =
    for
      rawParameters <- parseArgs(args)
      parameters = normalizeAliases(rawParameters)
      _ <- requireParameters(parameters, "mySqlHost", "mySqlUser", "mySqlPassword", "mySqlDbname", "sqlfs", "index", "fieldToIndex")
      _ = logParameters(parameters)
      jsonFields <- parseJsonFieldMapping(parameters.get("jsonFieldFile"))
      sqlFiles <- Tools.parseSqlFileList(parameters("sqlfs"))
      importFields <- parseImportFieldsParameter(parameters.get("importFields"))
      fieldToIndex = parameters("fieldToIndex")
      _ <- validateFieldToIndex(importFields, fieldToIndex)
      repetitiveFields = parseRepetitiveFields(parameters.get("repetitiveField"))
      repetitiveSep = parameters.get("repetitiveSep").orElse(repetitiveFields.map(_ => "//@//"))
      conf = MySqlProducerConfig(
        mySqlHost = parameters("mySqlHost"),
        mySqlPort = parameters.getOrElse("mySqlPort", "3306").toInt,
        mySqlDbname = parameters("mySqlDbname"),
        mySqlUser = parameters("mySqlUser"),
        mySqlPassword = parameters("mySqlPassword"),
        sqlfs = sqlFiles,
        sqlEncoding = parameters.getOrElse("sqlEncoding", "utf-8"),
        jsonFields = jsonFields,
        repetitiveFields = repetitiveFields,
        repetitiveSep = repetitiveSep
      )
      _ <- indexRecords(conf, parameters("index"), fieldToIndex, importFields)
    yield ()

  /**
   * Parses command-line arguments into a name/value map.
   *
   * @param args command-line arguments received by the utility
   * @return parsed parameter map
   */
  private def parseArgs(args: Array[String]): Try[Map[String, String]] =
    Success(args.foldLeft(Map.empty[String, String]):
      case (map, par) =>
        val split = par.split(" *= *", 2)
        if split.size == 1 then map + (split(0).substring(2) -> "")
        else map + (split(0).substring(1) -> split(1))
    )

  /**
   * Normalizes legacy MySQL2Lucene option names to the current SQL tool names.
   *
   * @param parameters parsed command-line parameter map
   * @return parameter map with aliases copied to their canonical names
   */
  private def normalizeAliases(parameters: Map[String, String]): Map[String, String] =
    Seq(
      "host" -> "mySqlHost",
      "port" -> "mySqlPort",
      "user" -> "mySqlUser",
      "pswd" -> "mySqlPassword",
      "dbnm" -> "mySqlDbname",
      "sqls" -> "sqlfs",
      "sqlf" -> "sqlfs"
    ).foldLeft(parameters):
      case (map, (alias, canonical)) =>
        if map.contains(canonical) then map
        else map.get(alias).map(value => map + (canonical -> value)).getOrElse(map)

  /**
   * Validates whether the required parameters are present.
   *
   * @param parameters parsed command-line parameter map
   * @param required parameter names that must be available
   * @return result indicating whether the validation succeeded
   */
  private def requireParameters(parameters: Map[String, String],
                                required: String*): Try[Unit] =
    if required.forall(parameters.contains) then Success(())
    else Failure(IllegalArgumentException(usageMessage))

  /**
   * Prints the resolved parameters to the standard output.
   *
   * @param parameters parsed command-line parameter map
   * @return no value; this method logs the effective parameters
   */
  private def logParameters(parameters: Map[String, String]): Unit =
    println("Parameters:")
    parameters.foreach(param => println(s"\t${param._1}=${param._2}"))
    println()

  /**
   * Parses the optional JSON field remapping configuration file.
   *
   * @param jsonFieldFile optional path to the remapping configuration
   * @return result containing the parsed JSON field mapping when available
   */
  private def parseJsonFieldMapping(jsonFieldFile: Option[String]): Try[Option[Map[String, Map[String, String]]]] =
    jsonFieldFile.fold[Try[Option[Map[String, Map[String, String]]]]](Success(None)):
      jFile =>
        Using(Source.fromFile(jFile)):
          _.getLines().foldLeft(Map.empty[String, Map[String, String]]):
            case (map, line) =>
              MysqlProducer.parseJsonFieldMappingLine(line) match
                case Some((colName, jsonName, newJsonName)) =>
                  val jsonMapping = map.getOrElse(colName, Map.empty) + (jsonName -> newJsonName)
                  map + (colName -> jsonMapping)
                case None => map
        .map(Some(_))

  /**
   * Loads the import field list from inline text or an external file.
   *
   * @param importFields raw importFields argument received from the command line
   * @return field list content ready to be parsed
   */
  private def readImportFields(importFields: String): Try[String] =
    if importFields.startsWith("file=") then
      Using(Source.fromFile(importFields.substring(5)))(_.mkString.trim)
    else Success(importFields)

  /**
   * Parses the optional importFields parameter into a field name set.
   *
   * @param importFields raw importFields parameter received from the command line
   * @return result containing the selected field names, or none when all fields should be imported
   */
  private def parseImportFieldsParameter(importFields: Option[String]): Try[Option[Set[String]]] =
    importFields.fold[Try[Option[Set[String]]]](Success(None)):
      raw =>
        for
          content <- readImportFields(raw.trim)
          fields <- parseImportFields(content)
        yield Some(fields)

  /**
   * Parses importFields content into a field name set.
   *
   * @param importFieldsContent raw field list content to parse
   * @return result containing the selected field names
   */
  private def parseImportFields(importFieldsContent: String): Try[Set[String]] =
    Try:
      val fields = importFieldsContent
        .split("\\s*(,|\r?\n\r?)\\s*")
        .iterator
        .map(_.trim)
        .filter(_.nonEmpty)
        .toSet

      if fields.isEmpty then
        throw IllegalArgumentException("Parameter importFields must contain at least one field name.")
      fields

  /**
   * Validates that the selected index field is included in importFields when it is configured.
   *
   * @param importFields optional field names selected for export
   * @param fieldToIndex field requested for Lucene text indexing
   * @return result indicating whether the validation succeeded
   */
  private def validateFieldToIndex(importFields: Option[Set[String]],
                                   fieldToIndex: String): Try[Unit] =
    importFields match
      case Some(selectedFields) if !selectedFields.contains(fieldToIndex) =>
        Failure(IllegalArgumentException(s"Field to index [$fieldToIndex] is not present in importFields."))
      case _ => Success(())

  /**
   * Indexes records selected by the configured SQL files into one Lucene index.
   *
   * @param conf base database configuration used during indexing
   * @param index index destination path
   * @param fieldToIndex document field indexed for similarity searches
   * @param importFields optional field names allowed in Lucene documents
   * @return result of indexing all SQL files
   */
  private def indexRecords(conf: MySqlProducerConfig,
                           index: String,
                           fieldToIndex: String,
                           importFields: Option[Set[String]]): Try[Unit] =
    print(s"Indexing ${conf.sqlf} ...")
    val producer: DocsProducer = filterProducer(new MysqlProducer(conf), importFields)
    val result: Try[Unit] = Tools.createLuceneIndex(producer, index, fieldToIndex, new NGAnalyzer())

    println(" OK")
    result

  /**
   * Parses the optional list of repetitive fields.
   *
   * @param repetitiveField raw repetitive field parameter
   * @return configured repetitive fields when available
   */
  private def parseRepetitiveFields(repetitiveField: Option[String]): Option[Set[String]] =
    Some(repetitiveField.getOrElse("title").trim.split(" *, *").toSet)

  /**
   * Wraps a producer so only importFields-selected fields are written to Lucene.
   *
   * @param producer original SQL-backed producer
   * @param importFields optional field names allowed by importFields
   * @return producer that emits filtered documents when importFields is configured
   */
  private def filterProducer(producer: DocsProducer,
                             importFields: Option[Set[String]]): DocsProducer =
    importFields match
      case Some(selectedFields) =>
        new DocsProducer:
          override def getDocuments: LazyList[Document] =
            producer.getDocuments.map:
              document =>
                Document(document.fields.filter(field => selectedFields.contains(field._1)))
      case None => producer
