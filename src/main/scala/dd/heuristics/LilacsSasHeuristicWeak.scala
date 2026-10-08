package dd.heuristics

import dd.interfaces.{CompResult, Document, Heuristics}
import dd.heuristics.Util.*

/**
 * Identifies LILACS/Sas duplicate pairs from title similarities and
 * bibliographic field equality using the weak heuristic variant.
 */
class LilacsSasHeuristicWeak extends Heuristics:
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
      val volume: (String, String) = Util.values(results, volumeField)
      val issue: (String, String) = Util.values(results, issueField)
      val author: (String, String) = Util.values(results, authorField)
      val pages: (String, String) = Util.values(results, pageField)

      isSimilar(results, journalField) &&
        Seq(
          equalOrAbsent(volume._1, volume._2),
          equalOrAbsent(issue._1, issue._2),
          equalOrAbsent(author._1, author._2),
          equalOrAbsent(pages._1, pages._2)
        ).forall(identity)
