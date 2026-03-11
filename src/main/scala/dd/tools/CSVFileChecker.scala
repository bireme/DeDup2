package dd.tools

import scala.annotation.unused
import scala.io.Source
import scala.util.{Failure, Success, Try, Using}

/**
 * Command-line utility that validates field counts in CSV files.
 *
 * The tool reads the input file lazily, checks whether each non-empty line has
 * the expected number of separated fields, and reports malformed records or
 * encoding-related issues found during the scan.
 */
object CSVFileChecker:
  /**
   * Prints the command usage information and exits.
   * @return no value; this method terminates the application
   */
  private val usageMessage: String =
    """usage: CSVFileChecker <options>
      |options:
      |	-csvFile=<path>          Path to the comma separated value file (csv)
      |	-numberOfFields=<num>    Number of fields in each line of the csv file
      |	[-fieldSeparator=<char>] Character indication the field separator. Default value is ','.
      |	[-encoding=<str>]        The csv character encoding. Default value is 'utf-8'
      |	[-reportFile=<path>]     Path to an output file having the check results. Default behaviour is to print the results to the standard output
      |	[-stopAfter=<num>]       Stop checking after <num> errors were found. Defeault behaviour is to check until the end of the input file.""".stripMargin

  /**
   * Entry point used when the utility is executed from the command line.
   *
   * @param args command-line arguments received by the utility
   * @return no value; this method delegates to the main workflow
   */
  def main(args: Array[String]): Unit =
    run(args) match
      case Success(_) => println("\nChecking process finished successfully!")
      case Failure(exception) => System.err.println(s"Checking error: ${exception.toString}")

  /**
   * Executes the CSV validation workflow.
   *
   * @param args command-line arguments received by the utility
   * @return result of validating the target CSV file
   */
  private def run(args: Array[String]): Try[Unit] =
    for
      parameters <- parseArgs(args)
      _ <- requireParameters(parameters, "csvFile", "numberOfFields")
      _ = logParameters(parameters)
      _ <- processFile(
        parameters("csvFile"),
        parameters("numberOfFields").toInt,
        parameters.getOrElse("fieldSeparator", ",").trim.head,
        parameters.getOrElse("encoding", "utf-8"),
        parameters.getOrElse("stopAfter", Integer.MAX_VALUE.toString).toInt,
        parameters.get("reportFile")
      )
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
   * Processes the CSV file and reports malformed records.
   *
   * @param csvFile CSV file path to inspect
   * @param numberOfFields expected number of fields per record
   * @param fieldSeparator field separator used in the CSV input
   * @param encoding character encoding used to read the input file
   * @param stopAfter value of stop after
   * @param reportFile optional path for the generated report
   * @return result of processing the CSV file
   */
  def processFile(csvFile: String,
                  numberOfFields: Int,
                  fieldSeparator: Char,
                  encoding: String,
                  stopAfter: Int,
                  @unused reportFile: Option[String]): Try[Unit] =
    Using(Source.fromFile(csvFile, encoding)):
      source =>
      require(numberOfFields > 0, "numberOfFields parameter should be a positive number")
      require(stopAfter > 0, "stopAfter parameter should be a positive number")

      val lines: Iterator[String] = source.getLines()
      val separator: String = fieldSeparator match
        case '|' => "\\|"
        case x => x.toString

      /**
       * Validates the current input line.
       *
       * @param line input line currently being validated
       * @param lineNumber line number associated with the current input line
       * @return no value; this method validates the current line in place
       */
      def inspectLine(line: String, lineNumber: Int): Unit =
        if line.trim.nonEmpty then
          val split: Array[String] = line.split(separator)
          if split.length != numberOfFields then
            println(s"Invalid number of fields. Line[$lineNumber]=$line Fields:${split.length}")

      /**
       * Iterates through the remaining input lines.
       *
       * @param curLine current line number in the input iteration
       * @return no value; this method advances the line iteration in place
       */
      def loop(curLine: Int): Unit =
        if hasNext(lines, curLine) then
          if curLine % 10000 == 0 then println(s"+++$curLine")

          Try(lines.next()) match
            case Success(line) => inspectLine(line, curLine)
            case Failure(exception) =>
              println(s"Possible character encoding error. Line[$curLine] Error:${exception.toString}")

          loop(curLine + 1)

      loop(curLine = 1)

  /**
   * Checks whether another input line is available.
   *
   * @param lines remaining lines available in the input source
   * @param curLine current line number in the input iteration
   * @return true when another input line is available, false otherwise
   */
  private def hasNext(lines: Iterator[String],
                      curLine: Int): Boolean =
    Try(lines.hasNext) match
      case Success(hasMore) => hasMore
      case Failure(exception) =>
        println(s"Stopping the checking process! Possible character encoding error. Line[$curLine] Error:${exception.toString}")
        throw exception
