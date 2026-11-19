package dd.tools

import scala.annotation.{tailrec, unused}
import scala.io.Source
import scala.util.{Failure, Success, Try, Using}

/**
 * Command-line utility that validates field counts in CSV files.
 *
 * The tool reads the input file lazily, checks whether each non-empty record has
 * the expected number of separated fields, and reports malformed records or
 * encoding-related issues found during the scan. Separators inside quoted
 * values are not counted, and a quoted value may span several lines, as in the
 * files written by SQL2CSV.
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
      |	[-fieldSeparator=<char>] Character indication the field separator. Default value is '|'.
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
        parameters.get("fieldSeparator").map(_.trim).flatMap(_.headOption).getOrElse(Tools.DefaultCsvFieldSeparator),
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
    Tools.parseCommandLineArgs(args)

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
   * @return result of processing the CSV file
   */
  private def processFile(csvFile: String,
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

      /**
       * Checks the number of fields of a complete record.
       *
       * @param startLine line where the record starts
       * @param separators number of separators found outside quotes
       */
      def inspectRecord(startLine: Int, separators: Int): Unit =
        val fields: Int = separators + 1
        if fields != numberOfFields then
          println(s"Invalid number of fields. Line[$startLine] Fields:$fields")

      /**
       * Iterates through the remaining input lines. A record whose quoted value
       * holds a line break continues on the next lines.
       *
       * @param curLine current line number in the input iteration
       * @param pending record still open inside quotes: start line and
       *                separators found so far
       * @return no value; this method advances the line iteration in place
       */
      @tailrec
      def loop(curLine: Int, pending: Option[(Int, Int)]): Unit =
        if hasNext(lines, curLine) then
          if curLine % 10000 == 0 then println(s"+++$curLine")

          val next: Option[(Int, Int)] =
            Try(lines.next()) match
              case Success(line) =>
                pending match
                  case None if line.trim.isEmpty => None
                  case _ =>
                    val (startLine, separators) = pending.getOrElse((curLine, 0))
                    val (lineSeparators, stillQuoted) = scanLine(line, fieldSeparator, inQuotes = pending.nonEmpty)
                    if stillQuoted then Some((startLine, separators + lineSeparators))
                    else
                      inspectRecord(startLine, separators + lineSeparators)
                      None
              case Failure(exception) =>
                println(s"Possible character encoding error. Line[$curLine] Error:${exception.toString}")
                pending

          loop(curLine + 1, next)
        else
          pending.foreach((startLine, _) => println(s"Unclosed quote. Record starting at line[$startLine]"))

      loop(curLine = 1, pending = None)

  /**
   * Counts the field separators of a line that are outside quoted values.
   *
   * Follows the quoting written by SQL2CSV (RFC 4180): a quote opens or closes
   * a quoted value and two consecutive quotes inside a quoted value are an
   * escaped quote.
   *
   * @param line line to scan
   * @param fieldSeparator field separator
   * @param inQuotes whether the line starts inside a quoted value (continuation
   *                 of the previous line)
   * @return separators found outside quotes, and whether the line ends inside
   *         a quoted value
   */
  private[tools] def scanLine(line: String,
                              fieldSeparator: Char,
                              inQuotes: Boolean): (Int, Boolean) =
    @tailrec
    def scan(index: Int, separators: Int, quoted: Boolean): (Int, Boolean) =
      if index >= line.length then (separators, quoted)
      else
        val char: Char = line.charAt(index)
        if char == '"' then
          if quoted && index + 1 < line.length && line.charAt(index + 1) == '"' then scan(index + 2, separators, quoted)
          else scan(index + 1, separators, !quoted)
        else if char == fieldSeparator && !quoted then scan(index + 1, separators + 1, quoted)
        else scan(index + 1, separators, quoted)

    scan(0, 0, inQuotes)

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
