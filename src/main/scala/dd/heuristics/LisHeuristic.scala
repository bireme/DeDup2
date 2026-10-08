package dd.heuristics

import dd.interfaces.{CompResult, Document, Heuristics}
import dd.heuristics.Util.*

/** Applies the LIS duplicate-detection rules to MongoDB documents. */
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
      val urls: (String, String) = Util.values(results, "link")
      equalAndPresent(urls._1, urls._2)
