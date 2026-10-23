package dd.comparators

import dd.interfaces.{CompResult, Comparator, Document, SimilarityStatus}
import dd.tools.Tools

import scala.util.Try
import scala.util.matching.Regex

/** Compares integer values extracted from fields, including canonical Roman numerals. */
class NumberComparator(fieldName: String,
                       regex: String = "\\d+") extends Comparator:
  require(fieldName.trim.nonEmpty, "fieldName must not be empty")
  require(regex.nonEmpty, "regex must not be empty")

  private val numberPattern: Regex = regex.r

  /**
   * Compares the first number extracted from the configured field in each document.
   *
   * The regular expression's first capture group is used when present; otherwise
   * the whole match is used. If no non-empty match is found, the complete field
   * value is checked as a Roman numeral.
   *
   * @param originalDoc source document
   * @param currentDoc candidate document
   * @return result with {@code yes}/{@code no} when both values resolve to numbers,
   *         or {@code undefined} when either value cannot be extracted
   */
  override def compare(originalDoc: Document,
                       currentDoc: Document): CompResult =
    val originalRaw: String = fieldValue(originalDoc)
    val currentRaw: String = fieldValue(currentDoc)
    val originalNumber: Option[Int] = extractNumber(originalRaw)
    val currentNumber: Option[Int] = extractNumber(currentRaw)

    val (score: Double, status: SimilarityStatus) = (originalNumber, currentNumber) match
      case (Some(left), Some(right)) if left == right => (1.0, SimilarityStatus.yes)
      case (Some(_), Some(_)) => (0.0, SimilarityStatus.no)
      case _ => (0.0, SimilarityStatus.undefined)

    CompResult(
      name = "NumberComparator",
      fieldName = fieldName,
      originalField = originalRaw,
      currentField = currentRaw,
      originalFieldOther = originalNumber.map(_.toString),
      currentFieldOther = currentNumber.map(_.toString),
      similarity = score,
      isSimilar = status
    )

  /** Returns the first value of the configured field, or an empty string if absent. */
  private def fieldValue(document: Document): String =
    document.fields.collectFirst:
      case (`fieldName`, value) => Option(value).getOrElse("")
    .getOrElse("")

  /** Extracts an integer from the field or parses the entire field as Roman numerals. */
  private def extractNumber(value: String): Option[Int] =
    numberPattern.findFirstMatchIn(value) match
      case Some(matched) =>
        val extracted: String =
          if matched.groupCount > 0 then Option(matched.group(1)).getOrElse("")
          else matched.matched
        if extracted.nonEmpty then Try(extracted.toInt).toOption else Tools.roman2Indu(value)
      case None => Tools.roman2Indu(value)
