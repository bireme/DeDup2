package dd.tools

import dd.producers.{MySqlProducerConfig, MysqlProducer}

import java.io.{BufferedWriter, FileWriter}
import scala.io.Source
import scala.util.{Failure, Success, Try, Using}

/**
 * Command-line utility that exports SQL query results to JSON.
 *
 * The tool loads the MySQL configuration from command-line parameters, executes
 * the configured SQL query through the shared producer, and serializes the
 * resulting internal documents into a JSON array written to disk.
 */
object SQL2Json:
  /**
   * Prints the command usage information and exits.
   * @return no value; this method terminates the application
   */
  private val usageMessage: String =
    """Export all SQL result records into a json file
      |usage: SQL2Json <options>
      |options:
      |	-mySqlHost=<host>       MySQL server host address
      |	-mySqlPort=<int>        MySQL server port
      |	-mySqlUser=<str>        MySQL database user
      |	-mySqlPassword=<str>    MySQL database password
      |	-mySqlDbname=<str>      MySQL database name
      |	-sqlfs=<name1>[,...,<nameN>] Comma-separated SQL statement files to execute sequentially.
      |	                        Results from each file are appended to the same output JSON array during this run.
      |	-outJsonFile=<path>     Path to the output JSON file
      |	[-jsonFieldFile=<path>] Path to a text file with JSON field mappings, one per line, using: <column name>=<json field name>[-><new field name>]
      |	                        For mapped SQL columns, object fields are extracted from the JSON content and emitted with the configured new field names.
      |	                        When <new field name> is omitted, the SQL column name is used as the output field name.
      |	                        Missing JSON fields are ignored. JSON array values are grouped with '//@//'; arrays of non-objects keep the SQL column name.
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
   * Executes the SQL-to-JSON export workflow.
   *
   * @param args command-line arguments received by the utility
   * @return result of exporting the selected records
   */
  private def run(args: Array[String]): Try[Unit] =
    for
      parameters <- parseArgs(args)
      _ <- requireParameters(parameters, "mySqlHost", "mySqlPort", "mySqlUser", "mySqlPassword", "mySqlDbname", "sqlfs", "outJsonFile")
      _ = logParameters(parameters)
      jFields <- parseJsonFieldMapping(parameters.get("jsonFieldFile"))
      sqlFiles <- Tools.parseSqlFileList(parameters("sqlfs"))
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
        repetitiveSep = None
      )
      _ <- exportRecords(conf, sqlFiles, parameters("outJsonFile"))
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
   * Exports the selected records to a JSON file.
   *
   * @param conf database configuration used during export
   * @param outJsonFile destination JSON file path
   * @return result of exporting the records
   */
  def exportRecords(conf: MySqlProducerConfig,
                    outJsonFile: String): Try[Unit] =
    exportRecords(conf, conf.sqlfs, outJsonFile)

  /**
   * Exports records selected by multiple SQL files to one JSON array file.
   *
   * @param conf base database configuration used during export
   * @param sqlFiles SQL files executed sequentially
   * @param outJsonFile destination JSON file path
   * @return result of exporting the records
   */
  def exportRecords(conf: MySqlProducerConfig,
                    sqlFiles: Seq[String],
                    outJsonFile: String): Try[Unit] =
    Try:
      val writer: BufferedWriter = new BufferedWriter(new FileWriter(outJsonFile))

      writer.write("[")

      var current: Int = 0
      val producer: MysqlProducer = new MysqlProducer(conf.copy(sqlfs = sqlFiles))
      producer.getDocuments.foreach:
        document =>
          if current % 1000 == 0 then println(s"+++$current")
          Try(Tools.doc2json(document)) match
            case Success(json) =>
              if current > 0 then writer.write(",")
              writer.newLine()
              writer.write(json.toString())
            case Failure(exception) =>
              System.err.println(s"Invalid document to json convertion. Document=$document Message=${exception.toString}")
          current += 1

      writer.newLine()
      writer.write("]")
      writer.close()
