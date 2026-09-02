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
      val titleSerialComp: Double = similarity(results, "title_serial")
      val issn: (String, String) = values(results, "issn")
      val volume: (String, String) = values(results, "volume_serial")
      val issue: (String, String) = values(results, "issue_number")
      val year: (String, String) = values(results, "publication_year")
      val matchingFields: Int = Seq(issn, volume, issue, year)
        .count((left, right) => equalAndPresent(left, right))

      if titleSerialComp == 1.0 then
        matchingFields >= 3
      else if titleSerialComp >= 0.8 && titleSerialComp < 1.0 then
        matchingFields == 4
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
