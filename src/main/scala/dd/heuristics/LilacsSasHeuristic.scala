package dd.heuristics

import dd.interfaces.{CompResult, Document, Heuristics}
import dd.heuristics.Util.*

/**
 * Applies the LILACS/Sas duplicate-detection rules to comparator results.
 *
 * <p>This class is the pipeline-oriented LILACS/Sas field heuristic.
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
  private val titleField: String = "title"
  private val journalField: String = "title_serial"
  private val yearField: String = "publication_year"
  private val volumeField: String = "volume_serial"
  private val issueField: String = "issue_number"
  private val authorField: String = "author"
  private val pageField: String = "pages"

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
      val titleComp: Double = similarity(results, titleField)
      val volume: (String, String) = values(results, volumeField)
      val issue: (String, String) = values(results, issueField)
      val year: (String, String) = values(results, yearField)
      val pages: (String, String) = values(results, pageField)
      val authorComp: Double = similarity(results, authorField)

      if titleComp == 1.0 then
        if journalSimilarity(results) == 1.0 then
          matchingFields(volume, issue, year, pages, authorComp) >= 3
        else if journalSimilarity(results) >= 0.8 && journalSimilarity(results) < 1.0 then
          matchingFields(volume, issue, year, pages, authorComp) >= 4
        else false
      else if titleComp >= 0.8 && titleComp < 1.0 then
        Seq(
          journalSimilarity(results) >= 0.8,
          equalAndPresent(volume._1, volume._2),
          equalAndPresent(issue._1, issue._2),
          equalAndPresent(pages._1, pages._2),
          equalAndPresent(year._1, year._2),
          authorComp >= 0.8
        ).count(identity) >= 5
      else false

  /**
   * Counts matching and present field pairs for an identical-title rule.
   *
   * @param volume compared volume values
   * @param issue compared issue values
   * @param year compared year values
   * @param pages compared page values
   * @param authorSimilarity author comparison score
   * @return number of satisfied field conditions
   */
  private def matchingFields(volume: (String, String),
                             issue: (String, String),
                             year: (String, String),
                             pages: (String, String),
                             authorSimilarity: Double): Int =
    Seq(
      equalAndPresent(volume._1, volume._2),
      equalAndPresent(issue._1, issue._2),
      equalAndPresent(pages._1, pages._2),
      equalAndPresent(year._1, year._2),
      authorSimilarity >= 0.8
    ).count(identity)

  /**
   * Returns the journal similarity score from the comparison results.
   *
   * @param results comparison results to search
   * @return journal similarity, or {@code 0.0} when absent
   */
  private def journalSimilarity(results: Seq[CompResult]): Double =
    similarity(results, journalField)

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
