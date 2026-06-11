package dd.comparators

import dd.interfaces.{CompResult, Comparator, Document}
import dd.tools.NGram
import dd.tools.StringSimilarity.DiceCoefficient
import dd.tools.Tools

/**
 * Comparator implementation based on n-gram similarity.
 *
 * This comparator serializes the configured field values, optionally applies
 * normalization, and computes an n-gram similarity score to determine whether
 * the compared documents satisfy the configured matching threshold.
 */
class NGramComparator(fieldName: String,
                      normalize: Boolean,
                      minSimilarity: Double) extends Comparator:
  require(minSimilarity <= 1.0)

  val fieldSeparator: String = "¦"
  override val isGate: Boolean = true

  /**
   * Compares the input documents and returns the comparison result.
   *
   * @param originalDoc source document used in the comparison
   * @param currentDoc candidate document being evaluated
   * @return comparison result describing the evaluated documents
   */
  override def compare(originalDoc: Document,
                       currentDoc: Document): CompResult =
    val oFields: Seq[String] = originalDoc.fields.filter(_._1.equals(fieldName)).map(_._2)
    val cFields: Seq[String] = currentDoc.fields.filter(_._1.equals(fieldName)).map(_._2)
    val oString: String = oFields.map(_.trim).mkString(fieldSeparator)
    val cString: String = cFields.map(_.trim).mkString(fieldSeparator)

    if normalize then
      val oStringNorm: String = Tools.normalizeStr(oString)
      val cStringNorm: String = Tools.normalizeStr(cString)
      val bothEmpty = oStringNorm.isEmpty && cStringNorm.isEmpty
      val nGramScore: Double = if bothEmpty then 0d else NGram.score(oStringNorm, cStringNorm)
      val finalScore: Double =
        if !bothEmpty && nGramScore >= minSimilarity then DiceCoefficient.score(oStringNorm, cStringNorm)
        else nGramScore

      CompResult("NGramComparator", fieldName, oString, cString, Some(oStringNorm), Some(cStringNorm), finalScore,
        !bothEmpty && nGramScore >= minSimilarity && finalScore >= minSimilarity)
    else
      val bothEmpty = oString.isEmpty && cString.isEmpty
      val nGramScore: Double = if bothEmpty then 0d else NGram.score(oString, cString)
      val finalScore: Double =
        if !bothEmpty && nGramScore >= minSimilarity then DiceCoefficient.score(oString, cString)
        else nGramScore

      CompResult("NGramComparator", fieldName, oString, cString, None, None, finalScore,
        !bothEmpty && nGramScore >= minSimilarity && finalScore >= minSimilarity)
