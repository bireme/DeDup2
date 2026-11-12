package dd.heuristics

import dd.interfaces.{CompResult, Document, Heuristics}
import dd.heuristics.Util.*

/**
 * Identifies LILACS/Sas duplicate pairs from title similarities and
 * bibliographic field equality using the weak heuristic variant.
 */
class LilacsSasHeuristicWeak extends Heuristics:
  //private val titleField: String = "title"
  //private val journalField: String = "title_serial"
  private val volumeField: String = "volume_serial"
  private val issueField: String = "issue_number"
  private val authorField: String = "author"
  private val pageField: String = "pages"
  private val urlField: String = "electronic_address"
  private val doiField: String = "doi"
  private val minTrueConditions: Int = 3

  /**
   * Determines whether the document pair is classified as duplicated.
   * Pairs whose journal titles differ are treated as different. Otherwise, at
   * least four of the five volume, issue, author, pages and URL conditions must
   * hold; undefined values satisfy a condition when the titles are identical.
   *
   * @param doc document associated with the comparison pair
   * @param results comparator results containing both compared field values
   *                and their similarity scores
   * @return {@code true} when the pair is classified as duplicated;
   *         {@code false} otherwise
   */
    override def isDuplicated(doc: Document,
                              results: Seq[CompResult]): Boolean =
      val similarFields: Seq[Boolean] = Seq(
        isSimilar(results, volumeField),
        isSimilar(results, issueField),
        isSimilar(results, authorField),
        isSimilar(results, pageField),
      )
      val similarFieldCount: Int = similarFields.count(identity)

      similarFieldCount >= minTrueConditions && (
        isSimilar(results, urlField) ||
        isSimilar(results, doiField) ||
        (isUndefined(results, urlField) && isUndefined(results, doiField) && similarFieldCount == similarFields.size) ||
        isUrlContentSimilar(results, urlField)
      )
