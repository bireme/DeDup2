package dd.heuristics

import dd.interfaces.{CompResult, Document, Heuristics}
import dd.heuristics.Util.*

/** Applies the LILACS/MNTam duplicate-detection rules to MongoDB documents. */
class LilacsMntamHeuristic extends Heuristics:
  /**
   * Classifies a document pair using its comparator results.
   *
   * @param doc document associated with the comparison pair
   * @param results comparator results for the pair
   * @return true when the pair is duplicated
   */
  override def isDuplicated(doc: Document, results: Seq[CompResult]): Boolean =
    if doc == null then false
    else
      val titleComp: Double = Util.similarity(results, "title")
      val titleMonographicComp: Double = Util.similarity(results, "title_monographic")
      val authorComp: Double = Util.similarity(results, "author")
      val authorAndPageScoresAreZero: Boolean =
        results.find(_.fieldName == "author").exists(_.similarity == 0.0) &&
          results.find(_.fieldName == "pages").exists(_.similarity == 0.0)
      val year: (String, String) = Util.values(results, "publication_year")
      val pages: (String, String) = Util.values(results, "pages")
      val authorMatch: Boolean = authorComp >= 0.8
      val matchingFields: Int = Seq(
        equalAndPresent(pages._1, pages._2),
        equalAndPresent(year._1, year._2),
        authorMatch
      ).count(identity)

      if authorAndPageScoresAreZero then false
      else if titleComp == 1.0 then
        if titleMonographicComp == 1.0 then
          matchingFields >= 2
        else matchingFields == 3
      else matchingFields == 3
