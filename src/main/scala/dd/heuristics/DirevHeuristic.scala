package dd.heuristics

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import dd.interfaces.{CompResult, Document, Heuristics}
import scala.io.Source
import scala.util.{Failure, Success, Try, Using}
import dd.heuristics.Util.*

/**
 * Applies the DIREV duplicate-detection rules to pipe-separated records.
 *
 * <p>Each input record must contain exactly seven fields in the order
 * represented by the position constants below. Records are classified using
 * the title similarity, dates, and URLs, and the result is written as
 * {@code id1|id2|duplicadas} or {@code id1|id2|diferentes}.</p>
 */
object DirevHeuristic:
  val id1Pos = 3
  val id2Pos = 4
  val titlePos = 11
  val date1Pos = 17
  val date2Pos = 18
  val url1Pos = 25
  val url2Pos = 26

  private val requiredFields: Int = List(id1Pos, id2Pos, titlePos, date1Pos, date2Pos, url1Pos, url2Pos).max

  private val duplicateStatus = "duplicadas"
  private val differentStatus = "diferentes"

  /**
   * Executes the DIREV heuristic from the command line.
   *
   * @param args command-line arguments in the form {@code <input> <output>}
   */
  def main(args: Array[String]): Unit =
    if args.length != 2 then
      Console.err.println("usage: DirevHeuristic <input-file> <output-file>")
      return

    run(Paths.get(args(0)), Paths.get(args(1))) match
      case Success(linesWritten) => println(s"$linesWritten lines processed")
      case Failure(exception) =>
        Console.err.println(s"Heuristic processing failed: ${exception.getMessage}")
        sys.exit(1)

  /**
   * Processes valid input records and writes their duplicate status.
   *
   * @param input path to the pipe-separated input file
   * @param output path to the output file
   * @return number of processed records, or a failure when file processing
   *         fails
   */
  def run(input: Path, output: Path): Try[Int] =
    Try:
      val outputParent = output.toAbsolutePath.normalize().getParent
      if outputParent != null then Files.createDirectories(outputParent)

      Using.resource(Source.fromFile(input.toFile, StandardCharsets.UTF_8.name())) { source =>
        Using.resource(Files.newBufferedWriter(output, StandardCharsets.UTF_8)) { writer =>
          var linesWritten = 0
          source.getLines().foreach { rawLine =>
            val line = rawLine.trim
            if line.nonEmpty then
              val fields = line.split("\\|", -1)
              if fields.length == requiredFields then
                val id1 = fields(id1Pos)
                val id2 = fields(id2Pos)
                val status = if isDuplicate(fields) then duplicateStatus else differentStatus
                writer.write(s"$id1|$id2|$status")
                writer.newLine()
                linesWritten += 1
          }
          linesWritten
        }
      }

  /**
   * Applies the identical-title and similar-title DIREV rules.
   *
   * @param fields parsed fields from one input record
   * @return {@code true} when the record satisfies a duplicate rule
   */
  private def isDuplicate(fields: Array[String]): Boolean =
    val titleComp = parseSimilarity(fields(titlePos))
    val date1 = fields(date1Pos)
    val date2 = fields(date2Pos)
    val url1 = fields(url1Pos)
    val url2 = fields(url2Pos)

    if titleComp == 1.0 then
      equalAndPresent(url1, url2)
    else if titleComp >= 0.7 && titleComp < 1.0 then
      equalAndPresent(date1, date2) && equalAndPresent(url1, url2)
    else false

/** Pipeline heuristic equivalent of the command-line DIREV heuristic. */
class DirevHeuristic extends Heuristics:
  /**
   * Classifies a document pair using title, date, and URL comparator results.
   *
   * @param doc document associated with the comparison pair
   * @param results comparator results for the pair
   * @return {@code true} when the pair satisfies a DIREV duplicate rule
   */
  override def isDuplicated(doc: Document, results: Seq[CompResult]): Boolean =
    if doc == null then false
    else
      val titleComp = similarity(results, "title")
      val dates = values(results, "start_date")
      val urls = values(results, "link")

      if titleComp == 1.0 then equalAndPresent(urls._1, urls._2)
      else if titleComp >= 0.7 && titleComp < 1.0 then
        equalAndPresent(dates._1, dates._2) && equalAndPresent(urls._1, urls._2)
      else false

  /**
   * Returns the similarity score for a comparator field.
   *
   * @param results comparator results to search
   * @param fieldName comparator field name
   * @return field similarity, or {@code 0.0} when absent
   */
  private def similarity(results: Seq[CompResult], fieldName: String): Double =
    results.find(_.fieldName == fieldName).map(_.similarity).getOrElse(0.0)

  /**
   * Returns both compared values for a comparator field.
   *
   * @param results comparator results to search
   * @param fieldName comparator field name
   * @return original and current values, or empty values when absent
   */
  private def values(results: Seq[CompResult], fieldName: String): (String, String) =
    results.find(_.fieldName == fieldName)
      .map(result => (result.originalField, result.currentField))
      .getOrElse(("", ""))

  /**
   * Checks whether two values are present and equal.
   *
   * @param left first value
   * @param right second value
   * @return {@code true} when both values are present and equal
   */
  private def equalAndPresent(left: String, right: String): Boolean =
    present(left) && present(right) && left.trim == right.trim

  /**
   * Checks whether a value is not empty and is not the {@code null} marker.
   *
   * @param value value to inspect
   * @return {@code true} when the value is present
   */
  private def present(value: String): Boolean =
    val normalized = value.trim
    normalized.nonEmpty && !normalized.equalsIgnoreCase("null")
