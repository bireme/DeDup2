package dd.tools

/**
 * Collection of reusable string-similarity algorithms.
 *
 * The object currently groups the Dice coefficient implementation used by
 * comparators that rely on pairwise character overlap for similarity scoring.
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
      val leftPairs = left.zip(left.tail).toSet
      val rightPairs = right.zip(right.tail).toSet

      if leftPairs.isEmpty && rightPairs.isEmpty then
        if left.sameElements(right) then 1.0 else 0.0
      else
        val intersection = leftPairs intersect rightPairs
        2.0 * intersection.size / (leftPairs.size + rightPairs.size)
