package dd.heuristics

import dd.interfaces.{CompResult, Document, Heuristics}
import dd.heuristics.Util.*

/**
 * Identifies LILACS/Sas duplicate pairs from title similarities and
 * bibliographic field equality.
 */
class LilacsSasHeuristic extends Heuristics:
  private val titleField: String = "title"
  private val journalField: String = "title_serial"
  private val volumeField: String = "volume_serial"
  private val issueField: String = "issue_number"
  private val authorField: String = "author"
  private val pageField: String = "pages"

  /**
   * Determines whether the document pair is classified as duplicated.
   * Pairs below either title-similarity threshold are treated as different.
   * Otherwise, pairs with all four bibliographic fields present and equal are
   * treated as duplicated; all other pairs are not classified as duplicates.
   *
   * @param doc document associated with the comparison pair
   * @param results comparator results containing both compared field values
   *                and their similarity scores
   * @return {@code true} when the pair is classified as duplicated;
   *         {@code false} otherwise
   */
  override def isDuplicated(doc: Document, results: Seq[CompResult]): Boolean =
    if doc == null then false
    else
      val titleComp: Double = similarity(results, titleField)
      val journalComp: Double = similarity(results, journalField)
      val volume: (String, String) = values(results, volumeField)
      val issue: (String, String) = values(results, issueField)
      val author: (String, String) = values(results, authorField)
      val pages: (String, String) = values(results, pageField)

      if titleComp < 0.8 || journalComp < 0.5 then false
      else
        val fieldsAreEqual: Boolean = Seq(
          equalAndPresent(volume._1, volume._2),
          equalAndPresent(issue._1, issue._2),
          equalAndPresent(author._1, author._2),
          equalAndPresent(pages._1, pages._2)
        ).forall(identity)
        fieldsAreEqual

  /**
   * Finds the comparison result for a configured field and returns its score.
   *
   * @param results comparison results to search
   * @param fieldName comparator field name
   * @return the similarity score, or {@code 0.0} when no result exists
   */
  private def similarity(results: Seq[CompResult], fieldName: String): Double =
    results.find(_.fieldName == fieldName).map(_.similarity).getOrElse(0.0)

  /**
   * Finds both compared values for a configured field.
   *
   * @param results comparison results to search
   * @param fieldName comparator field name
   * @return original and current values, or two empty values when absent
   */
  private def values(results: Seq[CompResult], fieldName: String): (String, String) =
    results.find(_.fieldName == fieldName)
      .map(result => (result.originalField, result.currentField))
      .getOrElse(("", ""))
