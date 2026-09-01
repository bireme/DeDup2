package dd.heuristics

import dd.interfaces.{CompResult, Document, Heuristics}
import dd.heuristics.Util.*

/**
 * Applies the LILACS/Sas duplicate-detection rules to comparator results.
 *
 * <p>This class is the pipeline-oriented counterpart of {@link LilacsSas}.
 * Instead of receiving a pipe-separated array, it receives the current
 * {@link Document} and the comparison results generated for the document pair.
 * The result fields are identified by their configured comparator field names:
 * {@code title}, {@code title_serial}, {@code publication_year},
 * {@code volume_serial}, {@code issue_number}, {@code author}, and
 * {@code pages}.</p>
 *
 * <p>The title, author, and field comparisons use the same similarity and
 * presence rules as the command-line object. A missing comparison result is
 * treated as a non-matching value.</p>
 */
class LilacsSasHeuristic extends Heuristics:
  private val titleField = "title"
  private val journalField = "title_serial"
  private val yearField = "publication_year"
  private val volumeField = "volume_serial"
  private val issueField = "issue_number"
  private val authorField = "author"
  private val pageField = "pages"

  /**
   * Determines whether the document pair satisfies a LILACS/Sas rule.
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
      val titleComp = similarity(results, titleField)
      val journal = values(results, journalField)
      val volume = values(results, volumeField)
      val issue = values(results, issueField)
      val year = values(results, yearField)
      val pages = values(results, pageField)
      val authorComp = similarity(results, authorField)
      val journalsEqual = equal(journal._1, journal._2)

      if titleComp == 1.0 then
        if journalsEqual then
          (equalAndPresent(volume._1, volume._2) &&
            equalAndPresent(issue._1, issue._2) &&
            equalAndPresent(pages._1, pages._2)) ||
            (missing(volume._1, volume._2) &&
              equalAndPresent(issue._1, issue._2) &&
              equalAndPresent(pages._1, pages._2) &&
              equalAndPresent(year._1, year._2) &&
              authorComp == 1.0) ||
            (missing(issue._1, issue._2) &&
              equalAndPresent(volume._1, volume._2) &&
              equalAndPresent(pages._1, pages._2) &&
              equalAndPresent(year._1, year._2) &&
              authorComp == 1.0) ||
            (missing(pages._1, pages._2) &&
              equalAndPresent(volume._1, volume._2) &&
              equalAndPresent(issue._1, issue._2) &&
              authorComp == 1.0)
        else
          equalAndPresent(volume._1, volume._2) &&
          equalAndPresent(issue._1, issue._2) &&
          equalAndPresent(pages._1, pages._2) &&
          authorComp == 1.0
      else if titleComp >= 0.8 && titleComp < 1.0 then
        present(journal._1) && present(journal._2) && journalsEqual &&
        equalAndPresent(volume._1, volume._2) &&
        equalAndPresent(issue._1, issue._2) &&
        equalAndPresent(pages._1, pages._2) &&
        authorComp == 1.0
      else false

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
