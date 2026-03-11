package dd.tools

/**
 * Utility object that computes similarities using n-gram overlap.
 *
 * It exposes a public entrypoint for string comparison and keeps the token
 * generation and overlap computation in private helpers for reuse.
 */
object NGram:
  /**
   * Computes the similarity score for the provided values.
   *
   * @param left first value used in the similarity computation
   * @param right second value used in the similarity computation
   * @param n size of the n-grams used in the computation
   * @return similarity score for the provided values
   */
  def score(left: String, right: String, n: Int = 1): Double =
    require(n > 0, "Ngram score, n-gram size must be a positive number.")
    nGram(left.toList, right.toList, n)

  /**
   * Computes the n-gram distance for the provided token lists.
   *
   * @param left first value used in the similarity computation
   * @param right second value used in the similarity computation
   * @param n size of the n-grams used in the computation
   * @return n-gram similarity score for the provided token lists
   */
  private def nGram[T](left: List[T], right: List[T], n: Int): Double =
    if left.length < n || right.length < n then
      0d
    else if left == right then
      tokenize(left, n).length.toDouble
    else
      val leftTokens = tokenize(left, n)
      val rightTokens = tokenize(right, n)
      val distance = leftTokens.intersect(rightTokens).length
      distance.toDouble / math.max(leftTokens.length, rightTokens.length)

  @annotation.tailrec
  /**
   * Splits the input into n-gram tokens.
   *
   * @param input input collection to tokenize
   * @param n size of the n-grams used in the computation
   * @param acc accumulator containing the tokens created so far
   * @return generated list of n-gram tokens
   */
  private def tokenize[T](input: List[T], n: Int, acc: List[List[T]] = Nil): List[List[T]] =
    if input.length <= n then acc :+ input
    else tokenize(input.tail, n, acc :+ input.take(n))
