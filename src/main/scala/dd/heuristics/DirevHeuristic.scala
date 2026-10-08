package dd.heuristics

import dd.interfaces.{CompResult, Document, Heuristics}
import dd.heuristics.Util.*

/**
 * Applies the DIREV duplicate-detection rules to MongoDB documents.
 *
 * <p>For identical series titles, both URLs must be present and equal. For
 * similar titles with a score from {@code 0.7} up to, but excluding, {@code
 * 1.0}, both dates and URLs must be present and equal.</p>
 */
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
      val titleComp: Double = Util.similarity(results, "title")
      val dates: (String, String) = Util.values(results, "start_date")
      val urls: (String, String) = Util.values(results, "link")

      if titleComp == 1.0 then equalAndPresent(urls._1, urls._2)
      else equalAndPresent(dates._1, dates._2) && equalAndPresent(urls._1, urls._2)
