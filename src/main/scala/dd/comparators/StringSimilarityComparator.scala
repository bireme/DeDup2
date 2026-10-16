package dd.comparators

import dd.interfaces.{CompResult, Comparator, Document, PreparedComparator, SimilarityStatus}
import dd.tools.Tools

/**
 * Common comparator implementation for configurable string similarities.
 *
 * <p>Field extraction, normalization and the algorithm-specific
 * preprocessing ({@code P}) of the source document run only once per
 * {@link prepare} call, and are reused for every candidate document.</p>
 *
 * @tparam P preprocessed representation consumed by the similarity algorithm
 */
abstract class StringSimilarityComparator[P](val fieldName: String,
                                             val normalize: Boolean,
                                             val minSimilarity: Double) extends Comparator:
  require(minSimilarity <= 1.0)

  private val fieldSeparator: String = "¦"

  /** Name exported in the comparison result. */
  protected def comparatorName: String

  /**
   * Converts a normalized, non-empty field value into the representation
   * consumed by {@link similarity}.
   *
   * @param value normalized field value
   * @return preprocessed value
   */
  protected def preprocess(value: String): P

  /**
   * Calculates the similarity of two preprocessed field values.
   *
   * @param left first preprocessed value
   * @param right second preprocessed value
   * @return similarity score
   */
  protected def similarity(left: P, right: P): Double

  /**
   * Normalizes a field before calculating similarity.
   *
   * @param value field value to normalize
   * @return normalized value
   */
  protected def normalizeValue(value: String): String =
    if normalize then Tools.normalizeStr(value) else value

  /**
   * Field value of one document at every processing stage.
   *
   * @param raw trimmed values joined by the field separator
   * @param normalized normalized value
   * @param preprocessed preprocessed value, absent when the normalized value is empty
   */
  private case class FieldValue(raw: String, normalized: String, preprocessed: Option[P])

  /**
   * Extracts, normalizes and preprocesses the configured field of a document.
   *
   * @param document document that provides the field values
   * @return field value at every processing stage
   */
  private def fieldValue(document: Document): FieldValue =
    val raw: String = document.fields.filter(_._1 == fieldName).map(_._2).map(_.trim).mkString(fieldSeparator)
    val normalized: String = normalizeValue(raw)
    FieldValue(raw, normalized, Option.when(normalized.nonEmpty)(preprocess(normalized)))

  /**
   * Prepares the comparator for a source document, processing it only once.
   *
   * @param originalDoc source document used in every subsequent comparison
   * @return comparator bound to the source document
   */
  override def prepare(originalDoc: Document): PreparedComparator =
    val original: FieldValue = fieldValue(originalDoc)
    currentDoc => compareValues(original, fieldValue(currentDoc))

  /**
   * Compares the configured field values from two documents.
   *
   * @param originalDoc source document
   * @param currentDoc candidate document
   * @return structured comparison result
   */
  override def compare(originalDoc: Document, currentDoc: Document): CompResult =
    prepare(originalDoc).compare(currentDoc)

  /**
   * Builds the comparison result for two processed field values.
   *
   * @param original source document field value
   * @param current candidate document field value
   * @return structured comparison result
   */
  private def compareValues(original: FieldValue, current: FieldValue): CompResult =
    val (score, status) = (original.preprocessed, current.preprocessed) match
      case (Some(left), Some(right)) =>
        val comparedScore: Double = similarity(left, right)
        (comparedScore, if comparedScore >= minSimilarity then SimilarityStatus.yes else SimilarityStatus.no)
      case _ => (0.0, SimilarityStatus.undefined)

    CompResult(comparatorName, fieldName, original.raw, current.raw,
      Some(original.normalized), Some(current.normalized), score,
      status)
