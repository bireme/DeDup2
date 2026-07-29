package dd.tools

import dd.producers.{MySqlProducerConfig, MysqlProducer}
import dd.interfaces.Document
import play.api.libs.json.{JsArray, JsString, JsValue, Json}

import java.io.{BufferedWriter, FileWriter}
import scala.collection.mutable
import scala.io.Source
import scala.util.{Failure, Success, Try, Using}

/**
 * Command-line utility that exports SQL query results to CSV.
 *
 * The tool loads the MySQL configuration from command-line parameters, executes
 * the configured SQL query through the shared producer, and serializes the
 * resulting internal documents into a CSV file written to disk.
 */
object SQL2CSV:
  /**
   * Prints the command usage information and exits.
   * @return no value; this method terminates the application
   */
  private val usageMessage: String =
    """Export all SQL result records into a csv file
      |usage: SQL2CSV <options>
      |options:
      |	-mySqlHost=<host>       MySQL server host address
      |	-mySqlPort=<int>        MySQL server port
      |	-mySqlUser=<str>        MySQL database user
      |	-mySqlPassword=<str>    MySQL database password
      |	-mySqlDbname=<str>      MySQL database name
      |	-sqlfs=<name1>[,...,<nameN>] Comma-separated SQL statement files to execute sequentially.
      |	                        Results from each file are appended to the same output CSV during this run.
      |	-outCsvFile=<path>      Path to the output CSV file
      |	[-fieldSeparator=<char>] Character indicating the field separator. Default value is ','.
      |	[-jsonFieldFile=<path>] Path to a text file with JSON field mappings, one per line, using: <column name>=<json field name>[-><new field name>]
      |	                        For mapped SQL columns, object fields are extracted from the JSON content and emitted with the configured new field names.
      |	                        When <new field name> is omitted, the SQL column name is used as the output field name.
      |	                        Missing JSON fields are ignored. JSON array values are grouped with '//@//'; arrays of non-objects keep the SQL column name.
      |	[-splitDocumentField=<name>] JSON-array field whose occurrences are emitted as separate CSV records.
      |	[-sqlEncoding=<str>]    SQL file character encoding. Default is 'utf-8'""".stripMargin

  /**
   * Entry point used when the utility is executed from the command line.
   *
   * @param args command-line arguments received by the utility
   * @return no value; this method delegates to the main workflow
   */
  def main(args: Array[String]): Unit =
    run(args) match
      case Success(_) => println("MySQL records exported successfully!")
      case Failure(exception) => println(s"Exporting MySQL records failed: ${exception.toString}")

  /**
   * Executes the SQL-to-CSV export workflow.
   *
   * @param args command-line arguments received by the utility
   * @return result of exporting the selected records
   */
  def run(args: Array[String]): Try[Unit] =
    for
      parameters <- parseArgs(args)
      _ <- requireParameters(parameters, "mySqlHost", "mySqlPort", "mySqlUser", "mySqlPassword", "mySqlDbname", "sqlfs", "outCsvFile")
      _ = logParameters(parameters)
      jFields <- parseJsonFieldMapping(parameters.get("jsonFieldFile"))
      sqlFiles <- Tools.parseSqlFileList(parameters("sqlfs"))
      fieldSeparator <- parseFieldSeparator(parameters.get("fieldSeparator"))
      conf = MySqlProducerConfig(
        mySqlHost = parameters("mySqlHost"),
        mySqlPort = parameters("mySqlPort").toInt,
        mySqlDbname = parameters("mySqlDbname"),
        mySqlUser = parameters("mySqlUser"),
        mySqlPassword = parameters("mySqlPassword"),
        sqlfs = sqlFiles,
        sqlEncoding = parameters.getOrElse("sqlEncoding", "utf-8"),
        jsonFields = jFields,
        repetitiveFields = None,
        repetitiveSep = None,
        splitDocumentField = parameters.get("splitDocumentField").filter(_.nonEmpty)
      )
      _ <- exportRecords(conf, sqlFiles, parameters("outCsvFile"), fieldSeparator)
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
   * Resolves the configured CSV field separator.
   *
   * @param separator optional field separator argument
   * @return resolved single-character separator
   */
  private def parseFieldSeparator(separator: Option[String]): Try[Char] =
    separator.map(_.trim) match
      case None => Success(',')
      case Some(value) if value.length == 1 => Success(value.head)
      case Some(value) => Failure(IllegalArgumentException(s"Invalid fieldSeparator [$value]. Expected a single character."))

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
   * Exports the selected records to a CSV file.
   *
   * The CSV header is taken from the first returned document. The query is
   * expected to return the same columns for all rows, as SQL result sets
   * normally do.
   *
   * @param conf database configuration used during export
   * @param outCsvFile destination CSV file path
   * @param fieldSeparator CSV field separator
   * @return result of exporting the records
   */
  def exportRecords(conf: MySqlProducerConfig,
                    outCsvFile: String): Try[Unit] =
    exportRecords(conf, outCsvFile, ',')

  /**
   * Exports the selected records to a CSV file.
   *
   * @param conf database configuration used during export
   * @param outCsvFile destination CSV file path
   * @param fieldSeparator CSV field separator
   * @return result of exporting the records
   */
  def exportRecords(conf: MySqlProducerConfig,
                    outCsvFile: String,
                    fieldSeparator: Char): Try[Unit] =
    exportRecords(conf, conf.sqlfs, outCsvFile, fieldSeparator)

  /**
   * Exports records selected by multiple SQL files to one CSV file.
   *
   * @param conf base database configuration used during export
   * @param sqlFiles SQL files executed sequentially
   * @param outCsvFile destination CSV file path
   * @param fieldSeparator CSV field separator
   * @return result of exporting the records
   */
  def exportRecords(conf: MySqlProducerConfig,
                    sqlFiles: Seq[String],
                    outCsvFile: String): Try[Unit] =
    exportRecords(conf, sqlFiles, outCsvFile, ',')

  /**
   * Exports records selected by multiple SQL files to one CSV file.
   *
   * @param conf base database configuration used during export
   * @param sqlFiles SQL files executed sequentially
   * @param outCsvFile destination CSV file path
   * @param fieldSeparator CSV field separator
   * @return result of exporting the records
   */
  def exportRecords(conf: MySqlProducerConfig,
                    sqlFiles: Seq[String],
                    outCsvFile: String,
                    fieldSeparator: Char): Try[Unit] =
    Using(new BufferedWriter(new FileWriter(outCsvFile))):
      writer =>
        var header: Vector[String] = Vector.empty
        var current: Int = 0

        val producer = new MysqlProducer(conf.copy(sqlfs = sqlFiles))
        producer.getDocuments.foreach:
          document =>
            if current % 100000 == 0 then println(s"+++$current")

            Try(documentToFields(document)) match
              case Success(fields) =>
                if header.isEmpty then
                  header = fields.map(_._1).toVector
                  writer.write(csvRecord(header, fieldSeparator))
                  writer.newLine()

                val fieldMap = fields.toMap
                val row: String = csvRecord(header.map(column => fieldMap.getOrElse(column, "")), fieldSeparator)
                writer.write(row)
                writer.newLine()

              case Failure(exception) =>
                System.err.println(s"Invalid document to csv conversion. Document=$document Message=${exception.toString}")

            current += 1

  /**
   * Converts the internal document to flat fields while preserving field order.
   *
   * Repeated field names are grouped at the position of their first occurrence,
   * matching the JSON-array behavior previously used by this exporter without
   * losing the SQL result-set column order.
   */
  private[tools] def documentToFields(document: Document): Seq[(String, String)] =
    val grouped = mutable.LinkedHashMap.empty[String, Vector[JsValue]]
    document.fields.foreach:
      case (key, value) =>
        val current = grouped.getOrElse(key, Vector.empty)
        grouped.update(key, current :+ csvFieldValue(value))

    grouped.toSeq.map:
      case (key, values) =>
        val value =
          values match
            case Seq(single) => stringifyCsvFieldValue(single)
            case many => Json.stringify(JsArray(many))
        key -> value

  private def csvFieldValue(value: String): JsValue =
    val trimmed = Option(value).getOrElse("").trim
    if trimmed.startsWith("[") || trimmed.startsWith("{") then
      Try(Json.parse(trimmed)).getOrElse(JsString(trimmed))
    else JsString(trimmed)

  private def stringifyCsvFieldValue(value: JsValue): String =
    value match
      case JsString(text) => text
      case other => Json.stringify(other)

  /**
   * Escapes a value according to RFC 4180 CSV conventions.
   */
  private[tools] def csvEscape(value: String, fieldSeparator: Char): String =
    val normalized: String = Option(value).getOrElse("")
    val escaped: String = normalized.replace("\"", "\"\"")
    if escaped.exists(ch => ch == fieldSeparator || ch == '"' || ch == '\n' || ch == '\r') then "\"" + escaped + "\""
    else escaped

  /**
   * Formats one CSV record using the configured separator.
   */
  private[tools] def csvRecord(values: Iterable[String], fieldSeparator: Char): String =
    values.map(csvEscape(_, fieldSeparator)).mkString(fieldSeparator.toString)
