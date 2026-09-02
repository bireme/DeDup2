package dd.comparators

import dd.tools.StringSimilarity.LevenshteinJaccard
import dd.tools.Tools

/** Comparator based on weighted Levenshtein and word-Jaccard similarity. */
class LevenshteinJaccardComparator(fieldName: String,
                                   normalize: Boolean,
                                   minSimilarity: Double)
    extends StringSimilarityComparator(fieldName, normalize, minSimilarity):
  protected val comparatorName: String = "LevenshteinJaccardComparator"

  /**
   * Normalizes text while preserving word boundaries for the Jaccard component.
   *
   * @param value field value to normalize
   * @return normalized word-preserving value
   */
  override protected def normalizeValue(value: String): String =
    if normalize then Tools.normalizeWordsStr(value) else value

  /**
   * Calculates the weighted combined similarity for two prepared values.
   *
   * @param left first prepared value
   * @param right second prepared value
   * @return weighted similarity score
   */
  protected def similarity(left: String, right: String): Double =
    LevenshteinJaccard.score(left, right)
