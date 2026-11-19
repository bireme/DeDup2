package dd.heuristics

import dd.interfaces.{CompResult, Document, Heuristics}
import dd.heuristics.Util.*
import dd.interfaces.SimilarityStatus
import dd.interfaces.SimilarityStatus.*

/**
 * Identifies LILACS/Sas duplicate pairs from the electronic address or DOI
 * and, when they are not similar, from the number of different and undefined
 * comparator results, the title similarity, the pages and the final URL.
 */
class LilacsSasHeuristic extends Heuristics:
  private val titleField: String = "title"
  //private val journalField: String = "title_serial"
  private val pageField: String = "pages"
  private val urlField: String = "electronic_address"
  private val doiField: String = "doi_number"

  /**
   * Determines whether the document pair is classified as duplicated.
   *
   * A result is similar ({@code yes}), different ({@code no}) or undefined
   * (one of the values is empty). The count of different results considers
   * every comparator result (title, journal title, volume, issue, author,
   * pages, URL and DOI). The count of empty (undefined) fields leaves URL and
   * DOI out, so it does not depend on whether the configuration has
   * comparators for them; a missing URL or DOI comparator is treated as
   * undefined.
   *
   *  1. Every field in {@code requiredSimFields} must be similar (currently
   *     none; the journal title is not required).
   *  1. Similar URLs or similar DOIs are enough: the pair is duplicated
   *     whatever the other results are.
   *  1. Otherwise, at most one result may be different, and:
   *     - when both URL and DOI are undefined:
   *       - if the titles are identical (similarity 1.0, see {@code ~=}):
   *         either no result is different (empty fields are accepted), or
   *         no field other than URL and DOI is empty (journal title, volume,
   *         issue, author and pages are all filled) and the pages are
   *         similar, so that only one of the other fields differs;
   *       - otherwise (similar but not identical titles): no result may be
   *         different, at most one field other than URL and DOI may be empty
   *         and the pages must be similar. Parts of the same article ("I. Parte" and
   *         "II parte") are usually told apart only by their pages;
   *     - when the URL or the DOI is different (and therefore the other
   *       results have no difference): the URLs must end, after following
   *       their redirects, at the same host, port and path (see
   *       {@link Util.hasSameFinalUrl}; this step accesses the network). An
   *       undefined URL with a different DOI is never duplicated.
   *
   * There is no minimum number of similar results.
   *
   * @param doc document associated with the comparison pair
   * @param results comparator results containing both compared field values
   *                and their similarity scores
   * @return {@code true} when the pair is classified as duplicated;
   *         {@code false} otherwise
   */
  override def isDuplicated(doc: Document,
                            results: Seq[CompResult]): Boolean =
    val requiredSimFields: Seq[String] = Seq(/*journalField*/)
    val resultMap = results.foldLeft(Map[String, SimilarityStatus]()):
      case (map, res) => map + (res.fieldName -> res.isSimilar)

    if requiredSimFields.exists(fld => getStatus(fld, resultMap) != yes) then false
    else
      val urlStatus = getStatus(urlField, resultMap)
      val doiStatus = getStatus(doiField, resultMap)

      urlStatus == yes || doiStatus == yes ||
      {
        //val similarFieldCount: Int = resultMap.count((_, status) => status == yes)
        // URL and DOI are left out, so the count does not depend on whether
        // the configuration has comparators for them.
        val undefinedFieldCount: Int = resultMap.count((fld, status) =>
          status == undefined && fld != urlField && fld != doiField)
        val differentFieldCount: Int = resultMap.count((_, status) => status == no)
        val maxNoConditions: Int = 1

        (differentFieldCount <= maxNoConditions) && {
          if urlStatus == undefined && doiStatus == undefined then
            if similarity(results, titleField) ~= 1.0 then
              // No field is 'no' (empty fields allowed), or no field is empty:
              // every field is filled, the pages are 'yes' and at most one
              // other field is 'no'.
              (differentFieldCount == 0) || ((undefinedFieldCount == 0) && (getStatus(pageField, resultMap) == yes))
            else
              // No field is 'no', at most one field is empty and the pages
              // are 'yes'.
              (differentFieldCount == 0) && (undefinedFieldCount <= 1) && (getStatus(pageField, resultMap) == yes)
          else
            hasSameFinalUrl(results, urlField)
        }
      }

  private def getStatus(fieldName: String,
                        resultMap: Map[String, SimilarityStatus]): SimilarityStatus =
    resultMap.getOrElse(fieldName, undefined)
