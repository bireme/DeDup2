package dd

import dd.heuristics.{LilacsSasHeuristicStrong, LilacsSasHeuristic, LilacsSasHeuristicWeak, LilacsSasHeuristicWeak2}
import dd.interfaces.{CompResult, Document, SimilarityStatus}

class LilacsSasHeuristicSuite extends munit.FunSuite:
  private val candidate: Document = Document(Seq("id" -> "2"))

  /**
   * Creates the results required by the SAS heuristics.
   *
   * @param urlStatus status of the URL comparison
   * @param overrides statuses replacing the default {@code yes} of other fields
   * @param titleSimilarity similarity score of the title comparison
   */
  private def results(urlStatus: SimilarityStatus,
                      overrides: Map[String, SimilarityStatus] = Map.empty,
                      titleSimilarity: Double = 1.0): Seq[CompResult] =
    val title = result("title", SimilarityStatus.yes).copy(similarity = titleSimilarity)
    title +: Seq("title_serial", "volume_serial", "issue_number", "author", "pages")
      .map(field => result(field, overrides.getOrElse(field, SimilarityStatus.yes))) :+
      result("electronic_address", urlStatus)

  /** Creates one comparator result for the requested field and status. */
  private def result(fieldName: String, status: SimilarityStatus): CompResult =
    CompResult("TestComparator", fieldName, "value", "value", None, None,
      if status == SimilarityStatus.yes then 1.0 else 0.0, status)

  test("SAS heuristics accept matching bibliographic fields and URLs"):
    val resultsWithSameUrls: Seq[CompResult] = results(SimilarityStatus.yes)

    assert(new LilacsSasHeuristicStrong().isDuplicated(candidate, resultsWithSameUrls))
    assert(new LilacsSasHeuristic().isDuplicated(candidate, resultsWithSameUrls))
    assert(new LilacsSasHeuristicWeak().isDuplicated(candidate, resultsWithSameUrls))

  test("only the default and weak SAS heuristics accept matching bibliographic fields when URL is absent"):
    val resultsWithoutUrl: Seq[CompResult] = results(SimilarityStatus.undefined)

    assert(!new LilacsSasHeuristicStrong().isDuplicated(candidate, resultsWithoutUrl))
    assert(new LilacsSasHeuristic().isDuplicated(candidate, resultsWithoutUrl))
    assert(new LilacsSasHeuristicWeak().isDuplicated(candidate, resultsWithoutUrl))

  test("only the weak SAS heuristic accepts conflicting URLs when the other four conditions hold"):
    val resultsWithDifferentUrls: Seq[CompResult] = results(SimilarityStatus.no)

    assert(!new LilacsSasHeuristicStrong().isDuplicated(candidate, resultsWithDifferentUrls))
    assert(!new LilacsSasHeuristic().isDuplicated(candidate, resultsWithDifferentUrls))
    assert(new LilacsSasHeuristicWeak().isDuplicated(candidate, resultsWithDifferentUrls))

  test("default SAS heuristic requires all conditions"):
    val identicalTitleFailure = results(SimilarityStatus.yes, Map("pages" -> SimilarityStatus.no))
    val similarTitleFailure = results(SimilarityStatus.yes, Map("volume_serial" -> SimilarityStatus.undefined), 0.9)

    assert(!new LilacsSasHeuristic().isDuplicated(candidate, identicalTitleFailure))
    assert(!new LilacsSasHeuristic().isDuplicated(candidate, similarTitleFailure))

  test("weak SAS heuristic requires at least four true conditions with identical titles"):
    val oneFailure = results(SimilarityStatus.yes, Map("pages" -> SimilarityStatus.no))
    val twoFailures = results(SimilarityStatus.no, Map("pages" -> SimilarityStatus.no))

    assert(new LilacsSasHeuristicWeak().isDuplicated(candidate, oneFailure))
    assert(!new LilacsSasHeuristicWeak().isDuplicated(candidate, twoFailures))

  test("weak SAS heuristic requires at least four true conditions with similar titles"):
    val oneFailure = results(SimilarityStatus.yes, Map("volume_serial" -> SimilarityStatus.undefined), 0.9)
    val twoFailures = results(SimilarityStatus.yes,
      Map("volume_serial" -> SimilarityStatus.undefined, "author" -> SimilarityStatus.no), 0.9)

    assert(new LilacsSasHeuristicWeak().isDuplicated(candidate, oneFailure))
    assert(!new LilacsSasHeuristicWeak().isDuplicated(candidate, twoFailures))

  test("weak2 SAS heuristic requires exactly one false condition with identical titles"):
    val noFailure = results(SimilarityStatus.yes)
    val oneFailure = results(SimilarityStatus.no)
    val twoFailures = results(SimilarityStatus.no, Map("pages" -> SimilarityStatus.no))

    assert(!new LilacsSasHeuristicWeak2().isDuplicated(candidate, noFailure))
    assert(new LilacsSasHeuristicWeak2().isDuplicated(candidate, oneFailure))
    assert(!new LilacsSasHeuristicWeak2().isDuplicated(candidate, twoFailures))

  test("weak2 SAS heuristic requires exactly one false condition with similar titles"):
    val noFailure = results(SimilarityStatus.yes, titleSimilarity = 0.9)
    val oneFailure = results(SimilarityStatus.yes, Map("volume_serial" -> SimilarityStatus.undefined), 0.9)
    val twoFailures = results(SimilarityStatus.yes,
      Map("volume_serial" -> SimilarityStatus.undefined, "author" -> SimilarityStatus.no), 0.9)

    assert(!new LilacsSasHeuristicWeak2().isDuplicated(candidate, noFailure))
    assert(new LilacsSasHeuristicWeak2().isDuplicated(candidate, oneFailure))
    assert(!new LilacsSasHeuristicWeak2().isDuplicated(candidate, twoFailures))

  test("default and weak SAS heuristics reject different journal titles"):
    val differentJournal = results(SimilarityStatus.yes, Map("title_serial" -> SimilarityStatus.no))

    assert(!new LilacsSasHeuristic().isDuplicated(candidate, differentJournal))
    assert(!new LilacsSasHeuristicWeak().isDuplicated(candidate, differentJournal))
    assert(!new LilacsSasHeuristicWeak2().isDuplicated(candidate, differentJournal))
