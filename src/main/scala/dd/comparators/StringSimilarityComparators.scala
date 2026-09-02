package dd.comparators

import dd.interfaces.{CompResult, Comparator, Document}
import dd.tools.Tools

/** Common comparator implementation for configurable string similarities. */
abstract class StringSimilarityComparator(val fieldName: String,
                                          val normalize: Boolean,
                                          val minSimilarity: Double) extends Comparator:
  require(minSimilarity <= 1.0)

  private val fieldSeparator: String = "¦"

  /** Name exported in the comparison result. */
  protected def comparatorName: String

  /** Calculates the similarity of two prepared field values. */
  protected def similarity(left: String, right: String): Double
  /**
   * Normalizes a field before calculating similarity.
   *
   * @param value field value to normalize
   * @return normalized value
   */
  protected def normalizeValue(value: String): String =
    if normalize then Tools.normalizeStr(value) else value

  /**
   * Compares the configured field values from two documents.
   *
   * @param originalDoc source document
   * @param currentDoc candidate document
   * @return structured comparison result
   */
  override def compare(originalDoc: Document, currentDoc: Document): CompResult =
    val original: String = originalDoc.fields.filter(_._1 == fieldName).map(_._2).map(_.trim).mkString(fieldSeparator)
    val current: String = currentDoc.fields.filter(_._1 == fieldName).map(_._2).map(_.trim).mkString(fieldSeparator)
    val normalizedOriginal: String = normalizeValue(original)
    val normalizedCurrent: String = normalizeValue(current)
    val bothEmpty: Boolean = normalizedOriginal.isEmpty && normalizedCurrent.isEmpty
    val score: Double = if bothEmpty then 0.0 else similarity(normalizedOriginal, normalizedCurrent)

    CompResult(comparatorName, fieldName, original, current,
      Some(normalizedOriginal), Some(normalizedCurrent), score,
      !bothEmpty && score >= minSimilarity)
