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
      Seq(
        isSimilar(results, "volume_serial"),
        isSimilar(results, "issue_number"),
        isSimilar(results, "author"),
        isSimilar(results, "pages_monographic")
      ).forall(identity)
