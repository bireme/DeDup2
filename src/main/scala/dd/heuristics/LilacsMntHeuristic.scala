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
      val volume: (String, String) = Util.values(results, "volume_serial")
      val issue: (String, String) = Util.values(results, "issue_number")
      val author: (String, String) = Util.values(results, "author")
      val pages: (String, String) = Util.values(results, "pages_monographic")

      Seq(
        equalAndPresent(volume._1, volume._2),
        equalAndPresent(issue._1, issue._2),
        equalAndPresent(author._1, author._2),
        equalAndPresent(pages._1, pages._2)
      ).forall(identity)
