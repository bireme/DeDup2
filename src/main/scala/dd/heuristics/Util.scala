package dd.heuristics

import dd.interfaces.{CompResult, SimilarityStatus}
import scala.util.Try

/** Shared comparison helpers used by document heuristics. */
object Util:
  /**
   * Returns the similarity score for a comparator field.
   *
   * @param results comparator results to search
   * @param fieldName comparator field name
   * @return the similarity score, or {@code 0.0} when no result exists
   */
  def similarity(results: Seq[CompResult], fieldName: String): Double =
    results.find(_.fieldName == fieldName).map(_.similarity).getOrElse(0.0)

  /**
   * Returns both values for a comparator field.
   *
   * @param results comparator results to search
   * @param fieldName comparator field name
   * @return original and current values, or two empty values when absent
   */
  def values(results: Seq[CompResult], fieldName: String): (String, String) =
    results.find(_.fieldName == fieldName)
      .map(result => (result.originalField, result.currentField))
      .getOrElse(("", ""))

  /**
   * Returns whether the comparator marked the specified field as similar.
   *
   * @param results comparator results to search
   * @param fieldName comparator field name
   * @return {@code true} only when the field result is {@code yes}
   */
  def isSimilar(results: Seq[CompResult], fieldName: String): Boolean =
    results.find(_.fieldName == fieldName)
      .exists(_.isSimilar == SimilarityStatus.yes)

  /**
   * Parses a textual similarity value.
   *
   * @param value textual similarity value
   * @return parsed similarity, or {@code NaN} when the value is invalid
   */
  def parseSimilarity(value: String): Double =
    Try(value.trim.toDouble).getOrElse(Double.NaN)

  /**
   * Compares two values after trimming surrounding whitespace.
   *
   * @param left first value
   * @param right second value
   * @return {@code true} when both normalized values are equal
   */
  def equal(left: String, right: String): Boolean =
    left.trim == right.trim

  /**
   * Determines whether a value is available for comparison.
   *
   * @param value value to inspect
   * @return {@code false} for an empty value or the marker {@code null}
   */
  def present(value: String): Boolean =
    val normalized: String = value.trim
    normalized.nonEmpty && !normalized.equalsIgnoreCase("null")

  /**
   * Checks whether two values are present and equal.
   *
   * @param left first value
   * @param right second value
   * @return {@code true} when neither value is missing and both are equal
   */
  def equalAndPresent(left: String, right: String): Boolean =
    present(left) && present(right) && equal(left, right)

  /**
   * Checks equality when both values are present; absent values do not fail the check.
   *
   * @param left first value
   * @param right second value
   * @return {@code true} when either value is absent or both values are equal
   */
  def equalOrAbsent(left: String, right: String): Boolean =
    if present(left) && present(right) then equal(left, right) else true

  /**
   * Checks whether both values are missing.
   *
   * @param left first value
   * @param right second value
   * @return {@code true} when both values are empty or {@code null}
   */
  def missing(left: String, right: String): Boolean =
    !present(left) && !present(right)
