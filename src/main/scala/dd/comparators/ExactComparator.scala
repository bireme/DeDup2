package dd.comparators

import dd.interfaces.{CompResult, Comparator, Document}
import dd.tools.Tools

class ExactComparator(fieldName: String,
                      normalize: Boolean) extends Comparator {
  val fieldSeparator: String = "¦"

  override def compare(originalDoc: Document,
                       currentDoc: Document): CompResult = {
    val oFields: Seq[String] = originalDoc.fields.filter(_._1.equals(fieldName)).map(_._2)
    val cFields: Seq[String] = currentDoc.fields.filter(_._1.equals(fieldName)).map(_._2)
    val oString: String = oFields.map(_.trim).mkString(fieldSeparator)
    val cString: String = cFields.map(_.trim).mkString(fieldSeparator)

    if (normalize) {
      val oStringNorm: String = Tools.normalizeStr(oString)
      val cStringNorm: String = Tools.normalizeStr(cString)
      val similar: Boolean = oStringNorm.equals(cStringNorm)

      CompResult("ExactComparator", fieldName, oString, cString, Some(oStringNorm), Some(cStringNorm),
        if (similar) 1 else 0, similar)
    } else {
      val similar: Boolean = oString.equals(cString)

      CompResult("ExactComparator", fieldName, oString, cString, None, None, if (similar) 1 else 0, similar)
    }
  }
}
