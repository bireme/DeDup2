package dd.comparators

import dd.interfaces.{CompResult, Comparator, Document, SimilarityStatus}
import dd.tools.StringSimilarity.DiceCoefficient
import dd.tools.Tools

/**
 * Comparator implementation based on the Dice coefficient.
 *
 * This comparator concatenates the selected field values from both documents,
 * optionally normalizes the resulting strings, and computes a similarity score
 * using the Dice coefficient to decide whether the documents should match.
 */
class DiceComparator(val fieldName: String,
                     val normalize: Boolean,
                     val minSimilarity: Double) extends Comparator:
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
    val oString2: String = if normalize then Tools.normalizeStr(oString) else oString
    val cString2: String = if normalize then Tools.normalizeStr(cString) else cString
    val originalEmpty: Boolean = oString2.isEmpty
    val currentEmpty: Boolean = cString2.isEmpty
    val score: Double =
      if originalEmpty != currentEmpty then 0.0
      else if originalEmpty && currentEmpty then 1.0
      else DiceCoefficient.score(oString2, cString2)
    val status: SimilarityStatus =
      if originalEmpty && currentEmpty then SimilarityStatus.yes
      else if originalEmpty != currentEmpty then SimilarityStatus.undefined
      else if score >= minSimilarity then SimilarityStatus.yes
      else SimilarityStatus.no
    
    CompResult("DiceComparator", fieldName, oString, cString, Some(oString2), Some(cString2), score,
      status)
