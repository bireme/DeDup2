package dd.comparators

import dd.interfaces.{CompResult, Comparator, Document}
import dd.tools.Tools

/**
 * Comparator implementation that performs exact string matching.
 *
 * The comparator joins all values associated with the configured field,
 * optionally normalizes the resulting strings, and then checks whether both
 * serialized representations are exactly equal.
 */
class ExactComparator(fieldName: String,
                      normalize: Boolean) extends Comparator:
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
      val similar: Boolean = oStringNorm.equals(cStringNorm)

      CompResult("ExactComparator", fieldName, oString, cString, Some(oStringNorm), Some(cStringNorm),
        if similar then 1 else 0, similar)
    else
      val similar: Boolean = oString.equals(cString)

      CompResult("ExactComparator", fieldName, oString, cString, None, None, if similar then 1 else 0, similar)
