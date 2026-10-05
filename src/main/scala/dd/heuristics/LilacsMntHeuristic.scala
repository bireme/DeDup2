package dd.heuristics

import dd.interfaces.{CompResult, Document, Heuristics}
import dd.heuristics.Util.*

/** Applies the LILACS/MNT duplicate-detection rules to MongoDB documents. */
class LilacsMntHeuristic extends Heuristics:
  /**
   * Classifies a document pair using its comparator results.
   *
   * @param doc document associated with the comparison pair
   * @param results comparator results for the pair
   * @return true when the pair is duplicated
   */
  override def isDuplicated(doc: Document, results: Seq[CompResult]): Boolean =
    if doc == null then false
    else
      val titleComp: Double = score(results, "title_monographic")
      val authorComp: Double = score(results, "author")
      val volume: (String, String) = pair(results, "volume_serial")
      val issue: (String, String) = pair(results, "issue_number")
      val year: (String, String) = pair(results, "publication_year")
      val pages: (String, String) = pair(results, "pages_monographic")
      val authorMatch: Boolean = authorComp >= 0.8
      val matchingFields: Int = Seq(
        equalAndPresent(volume._1, volume._2),
        equalAndPresent(issue._1, issue._2),
        equalAndPresent(pages._1, pages._2),
        equalAndPresent(year._1, year._2),
        authorMatch
      ).count(identity)

      if titleComp == 1.0 then
        matchingFields >= 3
      else if titleComp >= 0.8 && titleComp < 1.0 then
        matchingFields >= 4
      else false

  /**
   * Returns a comparator similarity score or zero when absent.
   *
   * @param results comparison results to search
   * @param fieldName comparator field name
   * @return similarity score, or zero when absent
   */
  private def score(results: Seq[CompResult], fieldName: String): Double =
    results.find(_.fieldName == fieldName).map(_.similarity).getOrElse(0.0)

  /**
   * Returns the original and current values for a comparator field.
   *
   * @param results comparison results to search
   * @param fieldName comparator field name
   * @return original and current values
   */
  private def pair(results: Seq[CompResult], fieldName: String): (String, String) =
    results.find(_.fieldName == fieldName).map(result => (result.originalField, result.currentField)).getOrElse(("", ""))
