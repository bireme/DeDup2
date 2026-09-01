package dd.heuristics

import dd.interfaces.{CompResult, Document, Heuristics}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.io.Source
import scala.util.{Failure, Success, Try, Using}
import dd.heuristics.Util.*

/** Applies the LILACS/MNT duplicate rules to pipe-separated records. */
object LilacsMntHeuristic:
  val id1Pos = 3
  val id2Pos = 4
  val titlePos = 11
  val year1Pos = 15
  val year2Pos = 16
  val authorCompPos = 27
  val page1Pos = 31
  val page2Pos = 32
  val volume1Pos = 39
  val volume2Pos = 40
  val issue1Pos = 47
  val issue2Pos = 48

  private val requiredFields: Int = List(id1Pos, id2Pos, titlePos, year1Pos, year2Pos, authorCompPos, page1Pos, page2Pos,
                                         volume1Pos, volume2Pos, issue1Pos, issue2Pos).max

  /** Runs the MNT heuristic from the command line. */
  def main(args: Array[String]): Unit =
    if args.length != 2 then
      Console.err.println("usage: LilacsMntHeuristic <input-file> <output-file>")
      return
    run(Paths.get(args(0)), Paths.get(args(1))) match
      case Success(linesWritten) => println(s"$linesWritten lines processed")
      case Failure(exception) =>
        Console.err.println(s"Heuristic processing failed: ${exception.getMessage}")
        sys.exit(1)

  /** Processes valid MNT records and writes their duplicate status. */
  def run(input: Path, output: Path): Try[Int] =
    Try:
      val parent = output.toAbsolutePath.normalize().getParent
      if parent != null then Files.createDirectories(parent)
      Using.resource(Source.fromFile(input.toFile, StandardCharsets.UTF_8.name())) { source =>
        Using.resource(Files.newBufferedWriter(output, StandardCharsets.UTF_8)) { writer =>
          var count = 0
          source.getLines().foreach { rawLine =>
            val line = rawLine.trim
            if line.nonEmpty then
              val fields = line.split("\\|", -1)
              if fields.length >= requiredFields then
                val status = if isDuplicate(fields) then "duplicadas" else "diferentes"
                writer.write(s"${fields(id1Pos)}|${fields(id2Pos)}|$status")
                writer.newLine()
                count += 1
          }
          count
        }
      }

  /** Applies the MNT duplicate rules to one parsed record. */
  private def isDuplicate(fields: Array[String]): Boolean =
    val titleComp = parseSimilarity(fields(titlePos))
    val authorComp = parseSimilarity(fields(authorCompPos))
    val volume1 = fields(volume1Pos)
    val volume2 = fields(volume2Pos)
    val issue1 = fields(issue1Pos)
    val issue2 = fields(issue2Pos)
    val year1 = fields(year1Pos)
    val year2 = fields(year2Pos)
    val page1 = fields(page1Pos)
    val page2 = fields(page2Pos)
    val authorEqual = authorComp == 1.0

    if titleComp == 1.0 then
      (equalAndPresent(volume1, volume2) && equalAndPresent(issue1, issue2) && equalAndPresent(page1, page2)) ||
        (missing(volume1, volume2) && equalAndPresent(issue1, issue2) &&
          equalAndPresent(page1, page2) && equalAndPresent(year1, year2) && authorEqual) ||
        (missing(issue1, issue2) && equalAndPresent(volume1, volume2) &&
          equalAndPresent(page1, page2) && equalAndPresent(year1, year2) && authorEqual) ||
        (missing(page1, page2) && equalAndPresent(volume1, volume2) &&
          equalAndPresent(issue1, issue2) && authorEqual)
    else if titleComp >= 0.8 && titleComp < 1.0 then
      equalAndPresent(volume1, volume2) && equalAndPresent(issue1, issue2) &&
        equalAndPresent(page1, page2) && equalAndPresent(year1, year2) && authorEqual
    else false

/** Pipeline implementation of the LILACS/MNT heuristic. */
class LilacsMntHeuristic extends Heuristics:
  /** Classifies a document pair using its comparator results. */
  override def isDuplicated(doc: Document, results: Seq[CompResult]): Boolean =
    if doc == null then false
    else
      val titleComp = score(results, "title_monographic")
      val authorComp = score(results, "author")
      val volume = pair(results, "volume_serial")
      val issue = pair(results, "issue_number")
      val year = pair(results, "publication_year")
      val pages = pair(results, "pages_monographic")
      val authorEqual = authorComp == 1.0

      if titleComp == 1.0 then
        (equalAndPresent(volume._1, volume._2) && equalAndPresent(issue._1, issue._2) && equalAndPresent(pages._1, pages._2)) ||
          (missing(volume._1, volume._2) && equalAndPresent(issue._1, issue._2) && equalAndPresent(pages._1, pages._2) && equalAndPresent(year._1, year._2) && authorEqual) ||
          (missing(issue._1, issue._2) && equalAndPresent(volume._1, volume._2) && equalAndPresent(pages._1, pages._2) && equalAndPresent(year._1, year._2) && authorEqual) ||
          (missing(pages._1, pages._2) && equalAndPresent(volume._1, volume._2) && equalAndPresent(issue._1, issue._2) && authorEqual)
      else if titleComp >= 0.8 && titleComp < 1.0 then
        equalAndPresent(volume._1, volume._2) && equalAndPresent(issue._1, issue._2) && equalAndPresent(pages._1, pages._2) && equalAndPresent(year._1, year._2) && authorEqual
      else false

  /** Returns a comparator similarity score or zero when absent. */
  private def score(results: Seq[CompResult], fieldName: String): Double =
    results.find(_.fieldName == fieldName).map(_.similarity).getOrElse(0.0)

  /** Returns the original and current values for a comparator field. */
  private def pair(results: Seq[CompResult], fieldName: String): (String, String) =
    results.find(_.fieldName == fieldName).map(result => (result.originalField, result.currentField)).getOrElse(("", ""))
