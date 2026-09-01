package dd.heuristics

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import dd.interfaces.{CompResult, Document, Heuristics}
import scala.io.Source
import scala.util.{Failure, Success, Try, Using}
import dd.heuristics.Util.*

/**
 * Applies the LIS duplicate-detection rules to pipe-separated records.
 *
 * <p>Each input record must contain exactly five fields: two identifiers, the
 * title similarity, and the two compared URLs. The output contains the two
 * identifiers and the resulting duplicate status.</p>
 */
object LisHeuristic:
  val id1Pos = 3
  val id2Pos = 4
  val titleCompPos = 11
  val url1Pos = 17
  val url22Pos = 18

  private val requiredFields: Int = List(id1Pos, id2Pos, titleCompPos, url1Pos, url22Pos).max

  private val duplicateStatus = "duplicadas"
  private val differentStatus = "diferentes"

  /**
   * Executes the LIS heuristic from the command line.
   *
   * @param args command-line arguments in the form {@code <input> <output>}
   */
  def main(args: Array[String]): Unit =
    if args.length != 2 then
      Console.err.println("usage: LisHeuristic <input-file> <output-file>")
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
   * Applies the identical-title and similar-title LIS rules.
   *
   * @param fields parsed fields from one input record
   * @return {@code true} when both URLs are present and equal under a
   *         supported title-similarity range
   */
  private def isDuplicate(fields: Array[String]): Boolean =
    val titleComp = parseSimilarity(fields(titleCompPos))
    val url1 = fields(url1Pos)
    val url2 = fields(url22Pos)

    if titleComp == 1.0 then
      equalAndPresent(url1, url2)
    else if titleComp >= 0.9 && titleComp < 1.0 then
      equalAndPresent(url1, url2)
    else false

/** Pipeline heuristic equivalent of the command-line LIS heuristic. */
class LisHeuristic extends Heuristics:
  /**
   * Classifies a document pair using title and URL comparator results.
   *
   * @param doc document associated with the comparison pair
   * @param results comparator results for the pair
   * @return {@code true} when the pair satisfies a LIS duplicate rule
   */
  override def isDuplicated(doc: Document, results: Seq[CompResult]): Boolean =
    if doc == null then false
    else
      val titleComp = similarity(results, "title")
      val urls = values(results, "link")

      if titleComp == 1.0 then equalAndPresent(urls._1, urls._2)
      else if titleComp >= 0.9 && titleComp < 1.0 then equalAndPresent(urls._1, urls._2)
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
