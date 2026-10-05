package dd.comparators

import dd.interfaces.{CompResult, Comparator, Document, SimilarityStatus}
import dd.tools.Tools

/** Common comparator implementation for configurable string similarities. */
abstract class StringSimilarityComparator(val fieldName: String,
                                          val normalize: Boolean,
                                          val minSimilarity: Double) extends Comparator:
  require(minSimilarity <= 1.0)

  private val fieldSeparator: String = "¦"

  /** Name exported in the comparison result. */
  protected def comparatorName: String

  /**
   * Calculates the similarity of two prepared field values.
   *
   * @param left first prepared value
   * @param right second prepared value
   * @return similarity score
   */
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
    val originalEmpty: Boolean = normalizedOriginal.isEmpty
    val currentEmpty: Boolean = normalizedCurrent.isEmpty
    val comparedScore: Double =
      if originalEmpty || currentEmpty then 0.0
      else similarity(normalizedOriginal, normalizedCurrent)
    val status: SimilarityStatus =
      if originalEmpty && currentEmpty then SimilarityStatus.yes
      else if originalEmpty != currentEmpty then SimilarityStatus.undefined
      else if comparedScore >= minSimilarity then SimilarityStatus.yes
      else SimilarityStatus.no
    val score: Double =
      if status == SimilarityStatus.undefined then 0.0
      else if originalEmpty && currentEmpty then 1.0
      else comparedScore

    CompResult(comparatorName, fieldName, original, current,
      Some(normalizedOriginal), Some(normalizedCurrent), score,
      status)
