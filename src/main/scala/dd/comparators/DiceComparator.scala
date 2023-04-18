package dd.comparators

import com.github.vickumar1981.stringdistance.StringDistance.DiceCoefficient
import dd.interfaces.{CompResult, Comparator, Document}
import dd.tools.Tools

class DiceComparator(fieldName: String,
                     normalize: Boolean,
                     minSimilarity: Double) extends Comparator {
  require(minSimilarity <= 1.0)

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
      val diceCoefficient: Double = DiceCoefficient.score(oStringNorm, cStringNorm)

      CompResult("DiceComparator", fieldName, oString, cString, Some(oStringNorm), Some(cStringNorm), diceCoefficient,
        diceCoefficient >= minSimilarity)
    } else {
      val diceCoefficient: Double = DiceCoefficient.score(oString, cString)

      CompResult("DiceComparator", fieldName, oString, cString, None, None, diceCoefficient, diceCoefficient >= minSimilarity)
    }
  }
}