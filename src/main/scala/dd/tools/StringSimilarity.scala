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
   * <p>Character bigrams are compared as sets (repeated bigrams count once).
   * Each bigram is packed into a single {@code Int} ({@code c1 << 16 | c2}),
   * so the computation works over sorted primitive arrays without boxing,
   * tuples or hash sets.</p>
   */
  object DiceCoefficient:
    /**
     * Precomputed bigrams of a string, reusable across many comparisons.
     *
     * @param value string the bigrams were extracted from
     * @param pairs sorted distinct bigrams of {@code value}
     */
    final class Bigrams private[DiceCoefficient] (val value: String,
                                                  private[DiceCoefficient] val pairs: Array[Int])

    /**
     * Extracts the bigrams of a string so it can be compared many times.
     *
     * @param value string to preprocess
     * @return precomputed bigrams of {@code value}
     */
    def bigrams(value: String): Bigrams = Bigrams(value, distinctBigrams(value))

    /**
     * Computes the similarity score for the provided values.
     *
     * @param left first value used in the similarity computation
     * @param right second value used in the similarity computation
     * @return similarity score for the provided values
     */
    def score(left: String, right: String): Double =
      score(bigrams(left), bigrams(right))

    /**
     * Computes the similarity score for precomputed bigrams.
     *
     * @param left bigrams of the first value
     * @param right bigrams of the second value
     * @return similarity score, identical to {@code score(left.value, right.value)}
     */
    def score(left: Bigrams, right: Bigrams): Double =
      val leftPairs: Array[Int] = left.pairs
      val rightPairs: Array[Int] = right.pairs

      if leftPairs.isEmpty && rightPairs.isEmpty then
        if left.value == right.value then 1.0 else 0.0
      else
        2.0 * intersectionSize(leftPairs, rightPairs) / (leftPairs.length + rightPairs.length)

    /**
     * Builds the sorted array of distinct character bigrams of a string.
     *
     * @param value string to split into bigrams
     * @return sorted bigrams without repetitions, each packed into an {@code Int}
     */
    private def distinctBigrams(value: String): Array[Int] =
      if value.length < 2 then Array.emptyIntArray
      else
        val pairs: Array[Int] = new Array[Int](value.length - 1)
        var index: Int = 0
        while index < pairs.length do
          pairs(index) = (value.charAt(index) << 16) | value.charAt(index + 1)
          index += 1
        java.util.Arrays.sort(pairs)

        var distinct: Int = 1
        index = 1
        while index < pairs.length do
          if pairs(index) != pairs(distinct - 1) then
            pairs(distinct) = pairs(index)
            distinct += 1
          index += 1
        if distinct == pairs.length then pairs else java.util.Arrays.copyOf(pairs, distinct)

    /**
     * Counts the values present in both sorted arrays of distinct values.
     *
     * @param left first sorted array without repetitions
     * @param right second sorted array without repetitions
     * @return number of common values
     */
    private def intersectionSize(left: Array[Int], right: Array[Int]): Int =
      var leftIndex: Int = 0
      var rightIndex: Int = 0
      var common: Int = 0
      while leftIndex < left.length && rightIndex < right.length do
        val leftValue: Int = left(leftIndex)
        val rightValue: Int = right(rightIndex)
        if leftValue == rightValue then
          common += 1
          leftIndex += 1
          rightIndex += 1
        else if leftValue < rightValue then leftIndex += 1
        else rightIndex += 1
      common

  /** Computes edit distance and normalized similarity using Levenshtein's algorithm. */
  object Levenshtein:
    /**
     * Computes the minimum number of insertions, deletions, and substitutions
     * required to transform one string into another.
     *
     * <p>Uses two rows of the dynamic-programming matrix sized by the shorter
     * string and plain index loops, so no objects are allocated per cell.</p>
     *
     * @param left first string
     * @param right second string
     * @return Levenshtein edit distance
     */
    def distance(left: String, right: String): Int =
      // The distance is symmetric, so the shorter string defines the row size.
      val (outer, inner) = if left.length >= right.length then (left, right) else (right, left)
      val innerLength: Int = inner.length
      var previous: Array[Int] = new Array[Int](innerLength + 1)
      var current: Array[Int] = new Array[Int](innerLength + 1)

      var innerIndex: Int = 0
      while innerIndex <= innerLength do
        previous(innerIndex) = innerIndex
        innerIndex += 1

      var outerIndex: Int = 0
      while outerIndex < outer.length do
        val outerChar: Char = outer.charAt(outerIndex)
        current(0) = outerIndex + 1
        innerIndex = 0
        while innerIndex < innerLength do
          val substitutionCost: Int = if outerChar == inner.charAt(innerIndex) then 0 else 1
          current(innerIndex + 1) = math.min(
            math.min(current(innerIndex) + 1, previous(innerIndex + 1) + 1),
            previous(innerIndex) + substitutionCost
          )
          innerIndex += 1
        val temporary: Array[Int] = previous
        previous = current
        current = temporary
        outerIndex += 1

      previous(innerLength)

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
