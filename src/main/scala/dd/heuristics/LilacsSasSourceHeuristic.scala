package dd.heuristics

import dd.interfaces.{CompResult, Document, Heuristics}
import dd.heuristics.Util.*

/** Applies the LILACS/Sas Source rules to MongoDB documents. */
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
      val titleSerialComp: Double = Util.similarity(results, "title_serial")
      val issn: (String, String) = Util.values(results, "issn")
      val volume: (String, String) = Util.values(results, "volume_serial")
      val issue: (String, String) = Util.values(results, "issue_number")
      val year: (String, String) = Util.values(results, "publication_year")
      val matchingFields: Int = Seq(issn, volume, issue, year)
        .count((left, right) => equalAndPresent(left, right))

      if titleSerialComp == 1.0 then
        matchingFields >= 3
      else matchingFields == 4
