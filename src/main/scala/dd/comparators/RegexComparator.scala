package dd.comparators

import dd.interfaces.{CompResult, Comparator, Document}
import dd.tools.Tools

import scala.util.matching.Regex

class RegexComparator(fieldName: String,
                      normalize: Boolean,
                      regex: String,
                      compString: String) extends Comparator {
  val fieldSeparator: String = "¦"
  private val re: Regex = regex.r

  override def compare(originalDoc: Document,
                       currentDoc: Document): CompResult = {
    val oFields: Seq[String] = originalDoc.fields.filter(_._1.equals(fieldName)).map(_._2)
    val cFields: Seq[String] = currentDoc.fields.filter(_._1.equals(fieldName)).map(_._2)
    val oString: String = oFields.map(_.trim).mkString(fieldSeparator)
    val cString: String = cFields.map(_.trim).mkString(fieldSeparator)

    if (normalize) {
      val oStringNorm: String = Tools.normalizeStr(oString)
      val cStringNorm: String = Tools.normalizeStr(cString)
      val oResult: String = re.replaceAllIn(oStringNorm, compString)
      val cResult: String = re.replaceAllIn(cStringNorm, compString)
      val similar: Boolean = oResult.equals(cResult)

      CompResult("RegexComparator", fieldName, oString, cString, Some(oResult), Some(cResult), if (similar) 1 else 0,
        similar)
    } else {
      val oResult: String = re.replaceAllIn(oString, compString)
      val cResult: String = re.replaceAllIn(cString, compString)
      val similar: Boolean = oResult.equals(cResult)

      CompResult("RegexComparator", fieldName, oString, cString, Some(oResult), Some(cResult), if (similar) 1 else 0,
        similar)
    }
  }
}
