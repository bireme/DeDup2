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
    val oString2: String = if normalize then Tools.normalizeStr(oString) else oString
    val cString2: String = if normalize then Tools.normalizeStr(cString) else cString
    val bothEmpty = oString2.isEmpty && cString2.isEmpty
    val similar: Boolean = !bothEmpty && oString2.equals(cString2)

    CompResult("ExactComparator", fieldName, oString, cString, Some(oString2), Some(cString2),
        if similar then 1 else 0, similar)