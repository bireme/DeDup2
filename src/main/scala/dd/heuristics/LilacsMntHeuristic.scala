package dd.heuristics

import dd.interfaces.{CompResult, Document, Heuristics}
import dd.heuristics.Util.*

/** Identifies LILACS/MNT duplicates by title similarity and field equality. */
class LilacsMntHeuristic extends Heuristics:
  /**
   * Classifies a pair as duplicated when its monographic title similarity is
   * at least 0.8 and volume, issue, author, and monographic pages are all
   * present and equal in both documents.
   *
   * @param doc document associated with the comparison pair
   * @param results comparator results for the pair
   * @return true when the pair is duplicated
   */
  override def isDuplicated(doc: Document, results: Seq[CompResult]): Boolean =
    if doc == null then false
    else
      val titleComp: Double = score(results, "title_monographic")
      val volume: (String, String) = pair(results, "volume_serial")
      val issue: (String, String) = pair(results, "issue_number")
      val author: (String, String) = pair(results, "author")
      val pages: (String, String) = pair(results, "pages_monographic")

      titleComp >= 0.8 && Seq(
        equalAndPresent(volume._1, volume._2),
        equalAndPresent(issue._1, issue._2),
        equalAndPresent(author._1, author._2),
        equalAndPresent(pages._1, pages._2)
      ).forall(identity)

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
