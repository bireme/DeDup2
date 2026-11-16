package dd.heuristics

import dd.interfaces.{CompResult, Document, Heuristics}
import dd.heuristics.Util.*
import dd.interfaces.SimilarityStatus
import dd.interfaces.SimilarityStatus.*

/**
 * Identifies LILACS/Sas duplicate pairs from the electronic address or DOI
 * and, when they are not similar, from the overall count of similar and
 * different comparator results.
 */
class LilacsSasHeuristic extends Heuristics:
  private val titleField: String = "title"
  //private val journalField: String = "title_serial"
  private val urlField: String = "electronic_address"
  private val doiField: String = "doi_number"

  /**
   * Determines whether the document pair is classified as duplicated.
   *
   * The counts below consider every comparator result: title, journal title,
   * volume, issue, author, pages, URL and DOI. Undefined (empty) values are
   * neither similar nor different.
   *
   *  1. Every field in {@code requiredSimFields} must be similar (currently
   *     none; the journal title is no longer required).
   *  1. Similar URLs or similar DOIs are enough: the pair is duplicated
   *     whatever the other results are.
   *  1. Otherwise:
   *     - at least five results must be similar (title and journal title
   *       included);
   *     - at most one result may be different;
   *     - when both URL and DOI are undefined, the titles must be identical
   *       (similarity 1.0) or no result may be different;
   *     - when the URL or the DOI is different, the URLs must point to the
   *       same document according to {@link UrlIdentity}: identifiers in the
   *       URLs, then (accessing the network) final address after redirects,
   *       DOI/canonical address in the page metadata, and visible text.
   *
   * @param doc document associated with the comparison pair
   * @param results comparator results containing both compared field values
   *                and their similarity scores
   * @return {@code true} when the pair is classified as duplicated;
   *         {@code false} otherwise
   */
  override def isDuplicated(doc: Document,
                            results: Seq[CompResult]): Boolean =
    val minTrueConditions: Int = 5   // title and title_serial included
    val maxNoConditions: Int = 1
    val requiredSimFields: Seq[String] = Seq(/*journalField*/)
    val resultMap = results.foldLeft(Map[String, SimilarityStatus]()):
      case (map, res) => map + (res.fieldName -> res.isSimilar)

    if requiredSimFields.exists(fld => getStatus(fld, resultMap) != yes) then false
    else
      getStatus(urlField, resultMap) == yes ||
      getStatus(doiField, resultMap) == yes ||
      {
        val similarFieldCount: Int = resultMap.count((_, status) => status == yes)
        val differentFieldCount: Int = resultMap.count((_, status) => status == no)

        if similarFieldCount < minTrueConditions || differentFieldCount > maxNoConditions then false
        else
          if getStatus(urlField, resultMap) == undefined && getStatus(doiField, resultMap) == undefined then
            (similarity(results, titleField) ~= 1.0) || (differentFieldCount == 0)
          else
            isUrlContentSimilar(results, urlField)
      }

  private def getStatus(fieldName: String,
                        resultMap: Map[String, SimilarityStatus]): SimilarityStatus =
    resultMap.getOrElse(fieldName, undefined)
