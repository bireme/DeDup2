package dd.heuristics

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import dd.interfaces.{CompResult, Document, Heuristics}
import scala.io.Source
import scala.util.{Failure, Success, Try, Using}
import dd.heuristics.Util.*

/**
 * Applies the LILACS/Sas Source duplicate rules to pipe-separated records.
 *
 * <p>The input contains exactly eleven pipe-separated fields in the order
 * represented by the position constants below.</p>
 */
object LilacsSasSourceHeuristic:
  val id1Pos = 3
  val id2Pos = 4
  val titleSerialPos = 11
  val issn1Pos = 15
  val issn2Pos = 16
  val volume1Pos = 23
  val volume2Pos = 24
  val issue1Pos = 31
  val issue2Pos = 32
  val year1Pos = 39
  val year2Pos = 40

  private val requiredFields: Int = List(id1Pos, id2Pos, titleSerialPos, issn1Pos, issn2Pos, volume1Pos, volume2Pos,
                                         issue1Pos, issue2Pos, year1Pos, year2Pos).max

  private val duplicateStatus = "duplicadas"
  private val differentStatus = "diferentes"

  /**
   * Executes the Source heuristic from the command line.
   *
   * @param args command-line arguments in the form {@code <input> <output>}
   */
  def main(args: Array[String]): Unit =
    if args.length != 2 then
      Console.err.println("usage: LilacsSasSourceHeuristic <input-file> <output-file>")
      return

    run(Paths.get(args(0)), Paths.get(args(1))) match
      case Success(linesWritten) => println(s"$linesWritten lines processed")
      case Failure(exception) =>
        Console.err.println(s"Heuristic processing failed: ${exception.getMessage}")
        sys.exit(1)

  /**
   * Processes the input records and writes one classification per valid record.
   *
   * @param input path to the pipe-separated input file
   * @param output path to the output file
   * @return the number of processed records, or a failure when file processing
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
   * Applies the identical-title and similar-title Source rules.
   *
   * @param fields parsed fields from one input record
   * @return {@code true} when the record satisfies a duplicate rule
   */
  private def isDuplicate(fields: Array[String]): Boolean =
    val titleSerialComp = parseSimilarity(fields(titleSerialPos))
    val issn1 = fields(issn1Pos)
    val issn2 = fields(issn2Pos)
    val volume1 = fields(volume1Pos)
    val volume2 = fields(volume2Pos)
    val issue1 = fields(issue1Pos)
    val issue2 = fields(issue2Pos)
    val year1 = fields(year1Pos)
    val year2 = fields(year2Pos)
    val issnsEqual = equal(issn1, issn2)

    if titleSerialComp == 1.0 then
      if present(issn1) && present(issn2) && issnsEqual then true
      else if !issnsEqual then
        (equalAndPresent(volume1, volume2) &&
          equalAndPresent(issue1, issue2) &&
          equalAndPresent(year1, year2)) ||
          (missing(volume1, volume2) &&
            equalAndPresent(issue1, issue2) &&
            equalAndPresent(year1, year2)) ||
          (missing(issue1, issue2) &&
            equalAndPresent(volume1, volume2) &&
            equalAndPresent(year1, year2))
      else false
    else if titleSerialComp >= 0.8 && titleSerialComp < 1.0 then
      Seq((issn1, issn2), (volume1, volume2), (issue1, issue2), (year1, year2))
        .count((left, right) => equalAndPresent(left, right)) >= 3
    else false

/** Correctly spelled short alias for the Source heuristic. */
object LilacsSasSource:
  /**
   * Executes the Source heuristic with the correctly spelled object name.
   *
   * @param args command-line arguments in the form {@code <input> <output>}
   */
  def main(args: Array[String]): Unit = LilacsSasSourceHeuristic.main(args)

  /**
   * Processes the Source input and output files.
   *
   * @param input path to the pipe-separated input file
   * @param output path to the output file
   * @return the number of processed records, or a processing failure
   */
  def run(input: Path, output: Path): Try[Int] = LilacsSasSourceHeuristic.run(input, output)

/** Pipeline heuristic equivalent of the LILACS/Sas Source file heuristic. */
class LilacsSasSourceHeuristic extends Heuristics:
  /**
   * Classifies a document pair using serial-title, ISSN, volume, issue, and
   * publication-year comparator results.
   *
   * @param doc document associated with the comparison pair
   * @param results comparator results for the pair
   * @return {@code true} when the pair satisfies a Source duplicate rule
   */
  override def isDuplicated(doc: Document, results: Seq[CompResult]): Boolean =
    if doc == null then false
    else
      val titleSerialComp = similarity(results, "title_serial")
      val issn = values(results, "issn")
      val volume = values(results, "volume_serial")
      val issue = values(results, "issue_number")
      val year = values(results, "publication_year")
      val issnsEqual = equal(issn._1, issn._2)

      if titleSerialComp == 1.0 then
        if equalAndPresent(issn._1, issn._2) then true
        else if !issnsEqual then
          (equalAndPresent(volume._1, volume._2) &&
            equalAndPresent(issue._1, issue._2) &&
            equalAndPresent(year._1, year._2)) ||
            (missing(volume._1, volume._2) &&
              equalAndPresent(issue._1, issue._2) &&
              equalAndPresent(year._1, year._2)) ||
            (missing(issue._1, issue._2) &&
              equalAndPresent(volume._1, volume._2) &&
              equalAndPresent(year._1, year._2))
        else false
      else if titleSerialComp >= 0.8 && titleSerialComp < 1.0 then
        Seq(issn, volume, issue, year).count((left, right) => equalAndPresent(left, right)) >= 3
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
   * Compares two values after trimming surrounding whitespace.
   *
   * @param left first value
   * @param right second value
   * @return {@code true} when both values are equal
   */
  private def equal(left: String, right: String): Boolean =
    left.trim == right.trim

  /**
   * Checks whether two values are present and equal.
   *
   * @param left first value
   * @param right second value
   * @return {@code true} when both values are present and equal
   */
  private def equalAndPresent(left: String, right: String): Boolean =
    present(left) && present(right) && equal(left, right)

  /**
   * Checks whether both values are missing.
   *
   * @param left first value
   * @param right second value
   * @return {@code true} when both values are empty or {@code null}
   */
  private def missing(left: String, right: String): Boolean =
    !present(left) && !present(right)

  /**
   * Determines whether a value is usable in a comparison.
   *
   * @param value value to inspect
   * @return {@code false} for empty values or the marker {@code null}
   */
  private def present(value: String): Boolean =
    val normalized = value.trim
    normalized.nonEmpty && !normalized.equalsIgnoreCase("null")
