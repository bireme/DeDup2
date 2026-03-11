package dd.comparators

import dd.interfaces.{CompResult, Comparator, Document}
import dd.tools.StringSimilarity.DiceCoefficient
import dd.tools.Tools

/**
 * Comparator implementation based on the Dice coefficient.
 *
 * This comparator concatenates the selected field values from both documents,
 * optionally normalizes the resulting strings, and computes a similarity score
 * using the Dice coefficient to decide whether the documents should match.
 */
class DiceComparator(fieldName: String,
                     normalize: Boolean,
                     minSimilarity: Double) extends Comparator:
  require(minSimilarity <= 1.0)

  val fieldSeparator: String = "¦"

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
      val diceCoefficient: Double = DiceCoefficient.score(oStringNorm, cStringNorm)

      CompResult("DiceComparator", fieldName, oString, cString, Some(oStringNorm), Some(cStringNorm), diceCoefficient,
        diceCoefficient >= minSimilarity)
    else
      val diceCoefficient: Double = DiceCoefficient.score(oString, cString)

      CompResult("DiceComparator", fieldName, oString, cString, None, None, diceCoefficient, diceCoefficient >= minSimilarity)
