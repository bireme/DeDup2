package dd.heuristics

import dd.interfaces.{CompResult, Document, Heuristics}
import dd.heuristics.Util.*
import dd.interfaces.SimilarityStatus
import dd.interfaces.SimilarityStatus.*

/**
 * Identifies LILACS/Sas duplicate pairs. The DOI decides first; then similar
 * URLs together with similar authors; otherwise the number of different and
 * empty comparator results, the title similarity and the pages.
 */
class LilacsSasHeuristic extends Heuristics:
  private val titleField: String = "title"
  //private val journalField: String = "title_serial"
  private val authorField: String = "author"
  private val pageField: String = "pages"
  private val urlField: String = "electronic_address"
  private val doiField: String = "doi_number"

  /**
   * Determines whether the document pair is classified as duplicated.
   *
   * A result is similar ({@code yes}), different ({@code no}) or undefined
   * (a value is empty or invalid, or the comparator is absent from the
   * configuration). A malformed DOI or a value without a valid URL is already
   * undefined in the results (see {@link dd.comparators.DoiComparator} and
   * {@link dd.comparators.UrlsComparator}). The URL comparator compares the
   * stored URLs and, as a last resort, the URLs reached after following their
   * redirects.
   *
   * The rules, in order:
   *  1. Every field in {@code requiredSimFields} must be similar (currently
   *     none; the journal title is not required).
   *  1. Similar DOIs: duplicated, whatever the other results are.
   *  1. Different DOIs: not duplicated, whatever the other results are.
   *  1. Similar URLs and similar authors: duplicated.
   *  1. Otherwise (DOI undefined), at most one result may be different. The
   *     count of different results includes the URL: a different URL is one
   *     difference, while a similar URL does not help. The count of empty
   *     (undefined) fields leaves URL and DOI out, so it does not depend on
   *     whether the configuration has comparators for them.
   *     - If the titles are identical (similarity 1.0, see {@code ~=}): either
   *       no result is different (empty fields are accepted), or no field
   *       other than URL and DOI is empty and the pages are similar, so that
   *       one field other than the pages differs.
   *     - Otherwise (similar but not identical titles): no result may be
   *       different, at most one field other than URL and DOI may be empty,
   *       and the pages must be similar. Parts of the same article ("I. Parte"
   *       and "II parte") are usually told apart only by their pages.
   *
   * There is no minimum number of similar results: identical titles with every
   * other field empty are duplicated.
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
      val doiStatus = getStatus(doiField, resultMap)
      lazy val urlStatus = getStatus(urlField, resultMap)
      lazy val authorStatus = getStatus(authorField, resultMap)
      lazy val differentFieldCount: Int = resultMap.count((_, status) => status == no)

      if doiStatus == yes then true
      else if doiStatus == no then false
      else if urlStatus == yes && authorStatus == yes && differentFieldCount == 0 then true
      else
        //val similarFieldCount: Int = resultMap.count((_, status) => status == yes)
        // Empty fields: URL and DOI are left out, so the count does not depend
        // on whether the configuration has comparators for them.
        val undefinedFieldCount: Int = resultMap.count((fld, status) =>
          status == undefined && fld != urlField && fld != doiField)
        // Different fields: the DOI is undefined here, a different URL counts.
        val maxNoConditions: Int = 1

        (differentFieldCount <= maxNoConditions) && {
            if similarity(results, titleField) ~= 1.0 then
              // No field is 'no' (empty fields allowed), or no field is empty:
              // every field is filled, the pages are 'yes' and one other field
              // (possibly the URL) is 'no'.
              (differentFieldCount == 0) || ((undefinedFieldCount == 0) && (getStatus(pageField, resultMap) == yes))
            else
              // No field is 'no', at most one field is empty and the pages
              // are 'yes'.
              (differentFieldCount == 0) && (undefinedFieldCount <= 1) && (getStatus(pageField, resultMap) == yes)
        }

  /**
   * Status of a field comparison.
   *
   * @param fieldName compared field
   * @param resultMap status of each compared field
   * @return the stored status, or {@code undefined} when the configuration has
   *         no comparator for the field
   */
  private def getStatus(fieldName: String,
                        resultMap: Map[String, SimilarityStatus]): SimilarityStatus =
    resultMap.getOrElse(fieldName, undefined)
