package dd.tools

import dd.NGAnalyzer
import dd.producers.CSVProducer

import scala.io.Source
import scala.util.{Failure, Success, Try, Using}

/**
 * Command-line utility that converts CSV input into a Lucene index.
 *
 * The tool parses the indexing parameters, loads the CSV schema from inline
 * text or file, and streams the resulting documents into a freshly created
 * Lucene index using the shared analyzer configuration.
 */
object CSV2Lucene:
  /**
   * Prints the command usage information and exits.
   * @return no value; this method terminates the application
   */
  private val usageMessage: String =
    """usage: CSV2Lucene <options>
      |options:
      |	-csvFile=<path> Path to the comma separated value file (csv)
      |	-index=<path> Path to the index to be created
      |	-schema=(<pos>=<fieldName>,...,<pos>=<fieldName>|file=<path>) Associate the csv field position (start with 0)
      |		with the Lucene document field name. If the parameter starts with file= then the corresponding schema file path will be used.
      |	-fieldToIndex=<name> Name of the field to be indexed
      |	[-fieldSeparator=<char>] Character indication the field separator. Default value is ','.
      |	[-encoding=<str>] The csv character encoding. Default value is 'utf-8'
      |	[--hasHeader] If present indicates the csv file has header.""".stripMargin

  /**
   * Entry point used when the utility is executed from the command line.
   *
   * @param args command-line arguments received by the utility
   * @return no value; this method delegates to the main workflow
   */
  def main(args: Array[String]): Unit =
    run(args) match
      case Success(_) => println("Indexing finished successfully!")
      case Failure(exception) => Console.err.println(exception.getMessage)

  /**
   * Executes the CSV-to-Lucene indexing workflow.
   *
   * @param args command-line arguments received by the utility
   * @return result of the complete indexing operation
   */
  private def run(args: Array[String]): Try[Unit] =
    for
      parameters <- parseArgs(args)
      _ <- requireParameters(parameters, "csvFile", "index", "schema", "fieldToIndex")
      _ = logParameters(parameters)
      schemaContent <- readSchema(parameters("schema").trim)
      schemaMap <- parseSchema(schemaContent)
      fieldToIndex = parameters("fieldToIndex")
      _ <- if schemaMap.values.exists(_ == fieldToIndex) then Success(()) else Failure(IllegalArgumentException(s"Field to index [$fieldToIndex] is not present in the schema file."))
      producer = new CSVProducer(
        parameters("csvFile"),
        schemaMap,
        parameters.contains("hasHeader"),
        parameters.getOrElse("fieldSeparator", ",").head,
        parameters.getOrElse("encoding", "utf-8")
      )
      _ = println("Indexing ...")
      _ <- Tools.createLuceneIndex(producer, parameters("index"), fieldToIndex, new NGAnalyzer())
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
   * Loads the schema definition from inline text or an external file.
   *
   * @param schema raw schema argument received from the command line
   * @return schema content ready to be parsed
   */
  private def readSchema(schema: String): Try[String] =
    if schema.startsWith("file=") then
      Using(Source.fromFile(schema.substring(5)))(_.mkString.trim)
    else Success(schema)

  /**
   * Parses the schema content into a positional field mapping.
   *
   * @param schemaContent raw schema content to parse
   * @return result containing the parsed schema map
   */
  private def parseSchema(schemaContent: String): Try[Map[Int, String]] =
    Try:
      schemaContent.split("\\s*(,|\r?\n\r?)\\s*").iterator.filter(_.nonEmpty).map(_.split(" *= *", 2)).map:
        case Array(index, field) => index.toInt -> field
        case other => throw IllegalArgumentException(s"Invalid schema entry: ${other.mkString("=")}")
      .toMap
