package dd.heuristics

import dd.interfaces.{CompResult, Document, Heuristics}

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
      val titleComp: Double = similarity(results, "title")
      val urls: (String, String) = values(results, "link")

      if titleComp == 1.0 then equalAndPresent(urls._1, urls._2)
      else if titleComp >= 0.9 && titleComp < 1.0 then equalAndPresent(urls._1, urls._2)
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

  /**
   * Checks whether two values are present and equal.
   *
   * @param left first value
   * @param right second value
   * @return {@code true} when both values are present and equal
   */
  private def equalAndPresent(left: String, right: String): Boolean =
    present(left) && present(right) && left.trim == right.trim

  /**
   * Checks whether a value is not empty and is not the {@code null} marker.
   *
   * @param value value to inspect
   * @return {@code true} when the value is present
   */
  private def present(value: String): Boolean =
    val normalized: String = value.trim
    normalized.nonEmpty && !normalized.equalsIgnoreCase("null")
