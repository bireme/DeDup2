package dd.heuristics

import scala.util.Try

/** Shared value-comparison helpers used by the LILACS/Sas heuristics. */
object Util:
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
   * Checks whether both values are missing.
   *
   * @param left first value
   * @param right second value
   * @return {@code true} when both values are empty or {@code null}
   */
  def missing(left: String, right: String): Boolean =
    !present(left) && !present(right)
