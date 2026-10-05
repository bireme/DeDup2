package dd.tools

import org.apache.commons.csv.{CSVFormat, CSVParser, CSVRecord}

import java.io.{BufferedWriter, FileWriter, StringReader}
import scala.jdk.CollectionConverters.*
import scala.io.Source
import scala.util.{Failure, Success, Try, Using}

final case class PeriodicalRef(
                                journal: String,
                                volume: Option[Int],
                                issue: Option[String],
                                startPage: Option[Int],
                                endPage: Option[Int],
                                month: Option[String],
                                year: Option[Int],
                                notes: List[String]
                              )

object SplitRef {
  private val DefaultFieldSeparator: Char = '|'

  private val usageMessage: String =
    """usage: SplitRef <options>
      |options:
      |	-csvFile=<path>              Path to the input CSV file
      |	-fieldPositions=<n1>[,...]   Zero-based field positions to convert using PeriodicalRef
      |	-outCsvFile=<path>           Path to the output CSV file
      |	[-fieldSeparator=<char>]     Character used as the CSV field separator. Default value is '|'.
      |
      |For each configured position, the original field is replaced by:
      |	journal, volume, issue, startPage, endPage, month, year, notes
      |using the selected field separator in the output CSV.""".stripMargin

  def usage(): String = usageMessage

  def main(args: Array[String]): Unit =
    run(args) match
      case Success(_) => println("References split successfully!")
      case Failure(exception) => System.err.println(s"SplitRef error: ${exception.toString}")

  def run(args: Array[String]): Try[Unit] =
    for
      parameters <- Tools.parseCommandLineArgs(args)
      _ <- requireParameters(parameters, "csvFile", "fieldPositions", "outCsvFile")
      positions <- parseFieldPositions(parameters("fieldPositions"))
      fieldSeparator <- parseFieldSeparator(parameters.get("fieldSeparator"))
      _ <- splitCsv(parameters("csvFile"), positions, parameters("outCsvFile"), fieldSeparator)
    yield ()

  def parseRef(input: String): Either[String, PeriodicalRef] =
    val JournalYearOnlyPattern =
      raw"""^\s*
           |(.+?)                             # periódico
           |\s*,\s*
           |(\d{4})                           # ano
           |\s*\.?
           |\s*$$
           |""".stripMargin.replaceAll("""\s+#.*""", "").replaceAll("\n", "").r

    val ReferencePattern =
      raw"""^\s*
           |([^;]+?)                         # periódico
           |\s*;
           |\s*(\d+)?                        # volume
           |\s*(?:\(([^)]+)\))?              # número
           |\s*(?::\s*(\d+)(?:\s*[-–]\s*(\d+))?)? # páginas
           |\s*(?:,\s*)?
           |\s*(?:([A-Za-z]{3,9})\.?\s*)?    # mês
           |(\d{4})?                         # ano
           |\s*\.?
           |\s*(.*?)                         # notas: tab, ilus etc.
           |\s*$$
           |""".stripMargin.replaceAll("""\s+#.*""", "").replaceAll("\n", "").r

    def optionalInt(value: String): Option[Int] =
      Option(value).filter(_.nonEmpty).map(_.toInt)

    input match
      case JournalYearOnlyPattern(journal, year) =>
        Right(
          PeriodicalRef(
            journal = journal.trim,
            volume = None,
            issue = None,
            startPage = None,
            endPage = None,
            month = None,
            year = optionalInt(year),
            notes = Nil
          )
        )

      case ReferencePattern(
        journal,
        volume,
        issue,
        startPage,
        endPage,
        month,
        year,
        notes
      ) =>
        Right(
          PeriodicalRef(
            journal = journal.trim,
            volume = optionalInt(volume),
            issue = Option(issue).map(_.trim).filter(_.nonEmpty),
            startPage = optionalInt(startPage),
            endPage = optionalInt(endPage),
            month = Option(month).map(_.trim).filter(_.nonEmpty),
            year = optionalInt(year),
            notes = Option(notes)
              .toList
              .flatMap(_.split("""[\s,;]+"""))
              .map(_.trim)
              .filter(_.nonEmpty)
          )
        )

      case _ =>
        Left(s"Referência bibliográfica inválida: '$input'")

  private def requireParameters(parameters: Map[String, String],
                                required: String*): Try[Unit] =
    if required.forall(parameters.contains) then Success(())
    else Failure(IllegalArgumentException(usageMessage))

  private def parseFieldPositions(value: String): Try[Set[Int]] =
    Try:
      val positions = value
        .split(",")
        .toSeq
        .map(_.trim)
        .filter(_.nonEmpty)
        .map(_.toInt)

      require(positions.nonEmpty, "fieldPositions must contain at least one position")
      require(positions.forall(_ >= 0), "fieldPositions must contain only zero-based non-negative positions")
      positions.toSet

  private def parseFieldSeparator(separator: Option[String]): Try[Char] =
    separator.map(_.trim).filter(_.nonEmpty) match
      case None => Success(DefaultFieldSeparator)
      case Some(value) if value.length == 1 => Success(value.head)
      case Some(value) => Failure(IllegalArgumentException(s"Invalid fieldSeparator [$value]. Expected a single character."))

  private def splitCsv(csvFile: String,
                       fieldPositions: Set[Int],
                       outCsvFile: String,
                       fieldSeparator: Char): Try[Unit] =
    Using.Manager:
      use =>
        val source = use(Source.fromFile(csvFile))
        val writer = use(new BufferedWriter(new FileWriter(outCsvFile)))

        source.getLines().zipWithIndex.foreach:
          case (line, index) =>
            val lineNumber = index + 1
            processLine(line, lineNumber, fieldPositions, fieldSeparator) match
              case Success(fields) =>
                writer.write(SQL2CSV.csvRecord(fields, fieldSeparator))
                writer.newLine()
              case Failure(exception) =>
                System.err.println(s"Invalid CSV record $lineNumber: $line")
                System.err.println(s"Message: ${exception.getMessage}")

  private def processLine(line: String,
                          lineNumber: Int,
                          fieldPositions: Set[Int],
                          fieldSeparator: Char): Try[Seq[String]] =
    for
      record <- parseLine(line, lineNumber, fieldSeparator)
      fields <- splitRecord(record, fieldPositions)
    yield fields

  private def parseLine(line: String,
                        lineNumber: Int,
                        fieldSeparator: Char): Try[CSVRecord] =
    Using(CSVParser.builder()
      .setFormat(CSVFormat.Builder.create().setDelimiter(fieldSeparator).setQuote(null).setTrim(true).get())
      .setReader(new StringReader(line))
      .get()):
      parser =>
        parser.getRecords.asScala.headOption
          .getOrElse(throw IllegalArgumentException(s"Empty CSV record at line $lineNumber"))

  private def splitRecord(record: CSVRecord,
                          fieldPositions: Set[Int]): Try[Seq[String]] =
    Try:
      record.asScala.toSeq.zipWithIndex.flatMap:
        case (value, index) if fieldPositions.contains(index) =>
          parseRef(value) match
            case Right(periodicalRef) => periodicalRefFields(periodicalRef)
            case Left(error) => throw IllegalArgumentException(s"$error at field $index")
        case (value, _) => Seq(value)

  private def periodicalRefFields(ref: PeriodicalRef): Seq[String] =
    Seq(
      ref.journal,
      ref.volume.map(_.toString).getOrElse(""),
      ref.issue.getOrElse(""),
      ref.startPage.map(_.toString).getOrElse(""),
      ref.endPage.map(_.toString).getOrElse(""),
      ref.month.getOrElse(""),
      ref.year.map(_.toString).getOrElse(""),
      ref.notes.mkString(" ")
    )
}
