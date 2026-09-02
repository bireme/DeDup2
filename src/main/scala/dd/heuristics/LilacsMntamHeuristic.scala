package dd.heuristics

import dd.interfaces.{CompResult, Document, Heuristics}
import dd.heuristics.Util.*

/** Applies the LILACS/MNTam duplicate-detection rules to MongoDB documents. */
class LilacsMntamHeuristic extends Heuristics:
  /** Classifies a document pair using its comparator results. */
  override def isDuplicated(doc: Document, results: Seq[CompResult]): Boolean =
    if doc == null then false
    else
      val titleComp: Double = score(results, "title")
      val titleMonographicComp: Double = score(results, "title_monographic")
      val year: (String, String) = pair(results, "publication_year")
      val pages: (String, String) = pair(results, "pages")
      val authorMatch: Boolean = score(results, "author") >= 0.8
      val matchingFields: Int = Seq(
        equalAndPresent(pages._1, pages._2),
        equalAndPresent(year._1, year._2),
        authorMatch
      ).count(identity)

      if titleComp == 1.0 then
        if titleMonographicComp == 1.0 then
          matchingFields >= 2
        else if titleMonographicComp >= 0.8 && titleMonographicComp < 1.0 then
          matchingFields == 3
        else false
      else if titleComp >= 0.8 && titleComp < 1.0 then
        titleMonographicComp >= 0.8 && matchingFields == 3
      else false

  /** Returns a comparator similarity score or zero when absent. */
  private def score(results: Seq[CompResult], fieldName: String): Double =
    results.find(_.fieldName == fieldName).map(_.similarity).getOrElse(0.0)

  /** Returns the original and current values for a comparator field. */
  private def pair(results: Seq[CompResult], fieldName: String): (String, String) =
    results.find(_.fieldName == fieldName).map(result => (result.originalField, result.currentField)).getOrElse(("", ""))
