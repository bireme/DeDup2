package dd.tools

/**
 * Collection of reusable string-similarity algorithms.
 *
 * The object groups reusable character- and word-based similarity algorithms.
 */
object StringSimilarity:
  /**
   * Utility object that computes similarities using the Dice coefficient.
   *
   * It exposes a public entrypoint for string comparison and keeps the token
   * pair computation in a private helper working over generic arrays.
   */
  object DiceCoefficient:
    /**
     * Computes the similarity score for the provided values.
     *
     * @param left first value used in the similarity computation
     * @param right second value used in the similarity computation
     * @return similarity score for the provided values
     */
    def score(left: String, right: String): Double =
      diceCoefficient(left.toCharArray, right.toCharArray)

    /**
     * Computes the Dice coefficient for the provided token arrays.
     *
     * @param left first value used in the similarity computation
     * @param right second value used in the similarity computation
     * @return Dice coefficient for the provided token arrays
     */
    private def diceCoefficient[T](left: Array[T], right: Array[T]): Double =
      val leftPairs: Set[(T, T)] =
        if left.length < 2 then Set.empty[(T, T)]
        else left.zip(left.tail).toSet

      val rightPairs: Set[(T, T)] =
        if right.length < 2 then Set.empty[(T, T)]
        else right.zip(right.tail).toSet

      if leftPairs.isEmpty && rightPairs.isEmpty then
        if left.sameElements(right) then 1.0 else 0.0
      else
        val intersection: Set[(T, T)] = leftPairs intersect rightPairs
        2.0 * intersection.size / (leftPairs.size + rightPairs.size)

  /** Computes edit distance and normalized similarity using Levenshtein's algorithm. */
  object Levenshtein:
    /**
     * Computes the minimum number of insertions, deletions, and substitutions
     * required to transform one string into another.
     *
     * @param left first string
     * @param right second string
     * @return Levenshtein edit distance
     */
    def distance(left: String, right: String): Int =
      val first: Array[Char] = left.toCharArray
      val second: Array[Char] = right.toCharArray
      var previous: Array[Int] = Array.tabulate(second.length + 1)(identity)
      var current: Array[Int] = new Array[Int](second.length + 1)

      first.zipWithIndex.foreach:
        case (leftChar, leftIndex) =>
          current(0) = leftIndex + 1
          second.zipWithIndex.foreach:
            case (rightChar, rightIndex) =>
              val substitutionCost: Int = if leftChar == rightChar then 0 else 1
              current(rightIndex + 1) = (current(rightIndex) + 1)
                .min(previous(rightIndex + 1) + 1)
                .min(previous(rightIndex) + substitutionCost)
          val temporary: Array[Int] = previous
          previous = current
          current = temporary

      previous(second.length)

    /**
     * Computes normalized Levenshtein similarity.
     *
     * @param left first string
     * @param right second string
     * @return value in the range {@code 0.0..1.0}, where {@code 1.0} means equal
     */
    def score(left: String, right: String): Double =
      val maximumLength: Int = left.length.max(right.length)
      if maximumLength == 0 then 1.0
      else 1.0 - distance(left, right).toDouble / maximumLength

  /** Computes Jaccard similarity over the distinct whitespace-separated words. */
  object Jaccard:
    /**
     * Computes word-based Jaccard similarity.
     *
     * @param left first string
     * @param right second string
     * @return intersection-over-union of the word sets
     */
    def score(left: String, right: String): Double =
      val leftWords: Set[String] = words(left)
      val rightWords: Set[String] = words(right)
      if leftWords.isEmpty && rightWords.isEmpty then 1.0
      else (leftWords intersect rightWords).size.toDouble / (leftWords union rightWords).size

    /**
     * Splits a string into distinct non-empty words.
     *
     * @param value string to tokenize
     * @return set of whitespace-separated words
     */
    private def words(value: String): Set[String] =
      value.trim.split("\\s+").iterator.filter(_.nonEmpty).toSet

  /** Combines normalized Levenshtein and word-based Jaccard similarities. */
  object LevenshteinJaccard:
    /**
     * Computes the weighted combined similarity.
     *
     * <p>The result is calculated as {@code 0.6 * Levenshtein + 0.4 * Jaccard}.
     * Both component scores are normalized to the range {@code 0.0..1.0}.</p>
     *
     * @param left first string
     * @param right second string
     * @return weighted similarity score in the range {@code 0.0..1.0}
     */
    def score(left: String, right: String): Double =
      0.6 * Levenshtein.score(left, right) + 0.4 * Jaccard.score(left, right)
