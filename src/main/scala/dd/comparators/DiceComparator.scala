package dd.comparators

import dd.tools.StringSimilarity.DiceCoefficient
import dd.tools.StringSimilarity.DiceCoefficient.Bigrams

/**
 * Comparator implementation based on the Dice coefficient.
 *
 * This comparator concatenates the selected field values from both documents,
 * optionally normalizes the resulting strings, and computes a similarity score
 * using the Dice coefficient to decide whether the documents should match.
 * The bigrams of the source document are computed once per {@code prepare}.
 */
class DiceComparator(fieldName: String,
                     normalize: Boolean,
                     minSimilarity: Double)
    extends StringSimilarityComparator[Bigrams](fieldName, normalize, minSimilarity):
  protected val comparatorName: String = "DiceComparator"

  /**
   * Extracts the bigrams of a normalized field value.
   *
   * @param value normalized field value
   * @return precomputed bigrams
   */
  protected def preprocess(value: String): Bigrams =
    DiceCoefficient.bigrams(value)

  /**
   * Calculates the Dice coefficient for two preprocessed values.
   *
   * @param left bigrams of the first value
   * @param right bigrams of the second value
   * @return Dice coefficient
   */
  protected def similarity(left: Bigrams, right: Bigrams): Double =
    DiceCoefficient.score(left, right)
