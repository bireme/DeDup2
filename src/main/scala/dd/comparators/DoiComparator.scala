package dd.comparators

import dd.interfaces.{CompResult, Comparator, Document, SimilarityStatus}

import scala.util.matching.Regex

/**
 * Comparator of DOI fields.
 *
 * Every value of the field (and every occurrence inside a value, separated by
 * {@code occSeparator}) must be a well-formed DOI, as checked by
 * {@link DoiComparator.isDoi}. When one of the documents has no value, or has
 * a value that is not a DOI, the result is {@code undefined}: a malformed DOI
 * is neither a similarity nor a difference. Otherwise the DOIs are compared
 * without the "doi:" and doi.org prefixes and ignoring case (DOIs are case
 * insensitive): the result is {@code yes} when both documents share a DOI and
 * {@code no} when they do not.
 *
 * @param fieldName name of the DOI field
 * @param occSeparator separator of the occurrences inside a field value
 */
class DoiComparator(fieldName: String,
                    occSeparator: String = "//@//") extends Comparator:
  /**
   * Compares the input documents and returns the comparison result.
   *
   * @param originalDoc source document used in the comparison
   * @param currentDoc candidate document being evaluated
   * @return comparison result describing the evaluated documents
   */
  override def compare(originalDoc: Document,
                       currentDoc: Document): CompResult =
    val oValues: Seq[String] = occurrences(originalDoc)
    val cValues: Seq[String] = occurrences(currentDoc)
    val oDois: Option[Seq[String]] = DoiComparator.dois(oValues)
    val cDois: Option[Seq[String]] = DoiComparator.dois(cValues)
    val status: SimilarityStatus =
      (oDois, cDois) match
        case (Some(original), Some(current)) =>
          if original.exists(current.contains) then SimilarityStatus.yes else SimilarityStatus.no
        case _ => SimilarityStatus.undefined
    val score: Int = if status == SimilarityStatus.yes then 1 else 0

    CompResult("DoiComparator", fieldName, oValues.mkString(occSeparator), cValues.mkString(occSeparator),
      oDois.map(_.mkString(occSeparator)), cDois.map(_.mkString(occSeparator)), score, status)

  /**
   * Reads the non-empty occurrences of the DOI field of a document.
   *
   * @param doc document to read
   * @return trimmed occurrences of every field value
   */
  private def occurrences(doc: Document): Seq[String] =
    doc.fields.collect { case (`fieldName`, value) => value }
      .flatMap(_.split(Regex.quote(occSeparator)))
      .map(_.trim)
      .filter(_.nonEmpty)

object DoiComparator:
  /**
   * A DOI ("10." + registrant code of 4 to 9 digits + "/" + suffix), optionally
   * preceded by "doi:" or by the doi.org resolver address. Group 1 is the DOI
   * without the prefix.
   */
  private val DoiRegex: Regex =
    """(?i)(?:doi:\s*|https?://(?:dx\.)?doi\.org/)?(10\.\d{4,9}/\S+)""".r

  /**
   * Checks whether a text is a well-formed DOI.
   *
   * @param value text to check (one occurrence)
   * @return true when the whole text matches the DOI regular expression
   */
  def isDoi(value: String): Boolean =
    DoiRegex.matches(value.trim)

  /**
   * Converts occurrences to DOIs without prefix, in lower case.
   *
   * @param occurrences field occurrences
   * @return the DOIs, or None when there is no occurrence or one of them is
   *         not a DOI
   */
  private[dd] def dois(occurrences: Seq[String]): Option[Seq[String]] =
    val converted: Seq[Option[String]] = occurrences.map:
      case DoiRegex(doi) => Some(doi.toLowerCase)
      case _ => None
    if converted.nonEmpty && converted.forall(_.nonEmpty) then Some(converted.flatten) else None
