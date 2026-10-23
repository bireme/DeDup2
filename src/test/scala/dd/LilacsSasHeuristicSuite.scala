package dd

import dd.heuristics.{LilacsSasHeuristic, LilacsSasHeuristicWeak}
import dd.interfaces.{CompResult, Document, SimilarityStatus}

class LilacsSasHeuristicSuite extends munit.FunSuite:
  private val candidate: Document = Document(Seq("id" -> "2"))

  /** Creates the results required by both SAS heuristics. */
  private def results(urlStatus: SimilarityStatus): Seq[CompResult] =
    Seq(
      result("title", SimilarityStatus.yes),
      result("title_serial", SimilarityStatus.yes),
      result("volume_serial", SimilarityStatus.yes),
      result("issue_number", SimilarityStatus.yes),
      result("author", SimilarityStatus.yes),
      result("pages", SimilarityStatus.yes),
      result("electronic_address", urlStatus)
    )

  /** Creates one comparator result for the requested field and status. */
  private def result(fieldName: String, status: SimilarityStatus): CompResult =
    CompResult("TestComparator", fieldName, "value", "value", None, None,
      if status == SimilarityStatus.yes then 1.0 else 0.0, status)

  test("SAS heuristics accept matching bibliographic fields when URL is absent"):
    val resultsWithoutUrl: Seq[CompResult] = results(SimilarityStatus.undefined)

    assert(new LilacsSasHeuristic().isDuplicated(candidate, resultsWithoutUrl))
    assert(new LilacsSasHeuristicWeak().isDuplicated(candidate, resultsWithoutUrl))

  test("SAS heuristics reject conflicting URLs"):
    val resultsWithDifferentUrls: Seq[CompResult] = results(SimilarityStatus.no)

    assert(!new LilacsSasHeuristic().isDuplicated(candidate, resultsWithDifferentUrls))
    assert(!new LilacsSasHeuristicWeak().isDuplicated(candidate, resultsWithDifferentUrls))
