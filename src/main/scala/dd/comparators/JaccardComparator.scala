package dd.comparators

import dd.tools.StringSimilarity.Jaccard
import dd.tools.Tools

/** Comparator based on Jaccard similarity of distinct words. */
class JaccardComparator(fieldName: String,
                        normalize: Boolean,
                        minSimilarity: Double)
    extends StringSimilarityComparator(fieldName, normalize, minSimilarity):
  protected val comparatorName: String = "JaccardComparator"

  /**
   * Normalizes text while preserving word boundaries for Jaccard comparison.
   *
   * @param value field value to normalize
   * @return normalized word-preserving value
   */
  override protected def normalizeValue(value: String): String =
    if normalize then Tools.normalizeWordsStr(value) else value

  /**
   * Calculates word-based Jaccard similarity for two prepared values.
   *
   * @param left first prepared value
   * @param right second prepared value
   * @return Jaccard similarity score
   */
  protected def similarity(left: String, right: String): Double =
    Jaccard.score(left, right)
