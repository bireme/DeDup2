package dd.comparators

import dd.tools.StringSimilarity.Levenshtein

/** Comparator based on normalized Levenshtein similarity. */
class LevenshteinComparator(fieldName: String,
                            normalize: Boolean,
                            minSimilarity: Double)
    extends StringSimilarityComparator[String](fieldName, normalize, minSimilarity):
  protected val comparatorName: String = "LevenshteinComparator"

  /**
   * Uses the normalized value itself as the preprocessed representation.
   *
   * @param value normalized field value
   * @return the same value
   */
  protected def preprocess(value: String): String = value

  /**
   * Calculates normalized Levenshtein similarity for two prepared values.
   *
   * @param left first prepared value
   * @param right second prepared value
   * @return normalized similarity score
   */
  protected def similarity(left: String, right: String): Double =
    Levenshtein.score(left, right)
