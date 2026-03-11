package dd.comparators

import dd.interfaces.{CompResult, Comparator, Document}
import dd.tools.Tools

import scala.util.matching.Regex

/**
 * Comparator implementation that rewrites fields through a regular expression.
 *
 * The comparator serializes the configured values, optionally normalizes them,
 * applies the configured regex replacement, and compares the transformed
 * results to determine whether both documents should be considered equal.
 */
class RegexComparator(fieldName: String,
                      normalize: Boolean,
                      regex: String,
                      compString: String) extends Comparator:
  val fieldSeparator: String = "¦"
  private val re: Regex = regex.r

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
      val oResult: String = re.replaceAllIn(oStringNorm, compString)
      val cResult: String = re.replaceAllIn(cStringNorm, compString)
      val similar: Boolean = oResult.equals(cResult)

      CompResult("RegexComparator", fieldName, oString, cString, Some(oResult), Some(cResult), if similar then 1 else 0,
        similar)
    else
      val oResult: String = re.replaceAllIn(oString, compString)
      val cResult: String = re.replaceAllIn(cString, compString)
      val similar: Boolean = oResult.equals(cResult)

      CompResult("RegexComparator", fieldName, oString, cString, Some(oResult), Some(cResult), if similar then 1 else 0,
        similar)
