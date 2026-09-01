package dd.heuristics

import dd.interfaces.{CompResult, Document, Heuristics}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.io.Source
import scala.util.{Failure, Success, Try, Using}
import dd.heuristics.Util.*

/** Applies the LILACS/MNTam duplicate rules to pipe-separated records. */
object LilacsMntamHeuristic:
  val id1Pos = 3
  val id2Pos = 4
  val titleCompPos = 11
  val titleMonographicCompPos = 19
  val year1Pos = 23
  val year2Pos = 24
  val authorCompPos = 35
  val page1Pos = 39
  val page2Pos = 40

  private val minimumFields: Int = List(id1Pos, id2Pos, titleCompPos, titleMonographicCompPos, year1Pos, year2Pos,
                                        authorCompPos, page1Pos, page2Pos).max

  /** Runs the MNTam heuristic from the command line. */
  def main(args: Array[String]): Unit =
    if args.length != 2 then
      Console.err.println("usage: LilacsMntamHeuristic <input-file> <output-file>")
      return
    run(Paths.get(args(0)), Paths.get(args(1))) match
      case Success(linesWritten) => println(s"$linesWritten lines processed")
      case Failure(exception) =>
        Console.err.println(s"Heuristic processing failed: ${exception.getMessage}")
        sys.exit(1)

  /** Processes valid MNTam records and writes their duplicate status. */
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
              if fields.length >= minimumFields then
                val status = if isDuplicate(fields) then "duplicadas" else "diferentes"
                writer.write(s"${fields(id1Pos)}|${fields(id2Pos)}|$status")
                writer.newLine()
                count += 1
          }
          count
        }
      }

  /** Applies the MNTam title, year, page, and author rules. */
  private def isDuplicate(fields: Array[String]): Boolean =
    val titleComp = parseSimilarity(fields(titleCompPos))
    val titleMonographicComp = parseSimilarity(fields(titleMonographicCompPos))
    val year1 = fields(year1Pos)
    val year2 = fields(year2Pos)
    val authorComp = parseSimilarity(fields(authorCompPos))
    val page1 = fields(page1Pos)
    val page2 = fields(page2Pos)
    val authorEqual = authorComp == 1.0

    if titleComp == 1.0 then
      if titleMonographicComp == 1.0 then
        equalAndPresent(year1, year2) && equalAndPresent(page1, page2) ||
          (missing(year1, year2) && equalAndPresent(page1, page2) && authorEqual) ||
          (missing(page1, page2) && equalAndPresent(year1, year2) && authorEqual)
      else
        equalAndPresent(year1, year2) && equalAndPresent(page1, page2) && authorEqual
    else if titleComp >= 0.8 && titleComp < 1.0 then
      titleMonographicComp == 1.0 &&
        equalAndPresent(year1, year2) && equalAndPresent(page1, page2) && authorEqual
    else false

/** Pipeline implementation of the LILACS/MNTam heuristic. */
class LilacsMntamHeuristic extends Heuristics:
  /** Classifies a document pair using its comparator results. */
  override def isDuplicated(doc: Document, results: Seq[CompResult]): Boolean =
    if doc == null then false
    else
      val titleComp = score(results, "title")
      val titleMonographicComp = score(results, "title_monographic")
      val year = pair(results, "publication_year")
      val pages = pair(results, "pages")
      val authorEqual = score(results, "author") == 1.0

      if titleComp == 1.0 then
        if titleMonographicComp == 1.0 then
          equalAndPresent(year._1, year._2) && equalAndPresent(pages._1, pages._2) ||
            (missing(year._1, year._2) && equalAndPresent(pages._1, pages._2) && authorEqual) ||
            (missing(pages._1, pages._2) && equalAndPresent(year._1, year._2) && authorEqual)
        else
          equalAndPresent(year._1, year._2) && equalAndPresent(pages._1, pages._2) && authorEqual
      else if titleComp >= 0.8 && titleComp < 1.0 then
        titleMonographicComp == 1.0 &&
          equalAndPresent(year._1, year._2) && equalAndPresent(pages._1, pages._2) && authorEqual
      else false

  /** Returns a comparator similarity score or zero when absent. */
  private def score(results: Seq[CompResult], fieldName: String): Double =
    results.find(_.fieldName == fieldName).map(_.similarity).getOrElse(0.0)

  /** Returns the original and current values for a comparator field. */
  private def pair(results: Seq[CompResult], fieldName: String): (String, String) =
    results.find(_.fieldName == fieldName).map(result => (result.originalField, result.currentField)).getOrElse(("", ""))
