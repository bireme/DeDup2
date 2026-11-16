package dd

import dd.heuristics.{LilacsSasHeuristicStrong, LilacsSasHeuristic, LilacsSasHeuristicWeak2}
import dd.interfaces.{CompResult, Document, SimilarityStatus}
import dd.interfaces.SimilarityStatus.{no, undefined, yes}

class LilacsSasHeuristicSuite extends munit.FunSuite:
  private val candidate: Document = Document(Seq("id" -> "2"))

  /**
   * Creates the results required by the SAS heuristics.
   *
   * @param urlStatus status of the URL comparison
   * @param overrides statuses replacing the default {@code yes} of other fields
   * @param titleSimilarity similarity score of the title comparison
   * @param doiStatus status of the DOI comparison
   */
  private def results(urlStatus: SimilarityStatus,
                      overrides: Map[String, SimilarityStatus] = Map.empty,
                      titleSimilarity: Double = 1.0,
                      doiStatus: SimilarityStatus = undefined): Seq[CompResult] =
    val title = result("title", overrides.getOrElse("title", yes)).copy(similarity = titleSimilarity)
    title +: Seq("title_serial", "volume_serial", "issue_number", "author", "pages")
      .map(field => result(field, overrides.getOrElse(field, yes))) :+
      result("electronic_address", urlStatus) :+
      result("doi_number", doiStatus)

  /**
   * Creates one comparator result for the requested field and status. The
   * values are not URLs, so the URL content check never downloads anything.
   */
  private def result(fieldName: String, status: SimilarityStatus): CompResult =
    CompResult("TestComparator", fieldName, "value", "value", None, None,
      if status == yes then 1.0 else 0.0, status)

  private def isDuplicated(results: Seq[CompResult]): Boolean =
    new LilacsSasHeuristic().isDuplicated(candidate, results)

  test("strong and default SAS heuristics accept matching bibliographic fields and URLs"):
    val resultsWithSameUrls: Seq[CompResult] = results(yes)

    assert(new LilacsSasHeuristicStrong().isDuplicated(candidate, resultsWithSameUrls))
    assert(isDuplicated(resultsWithSameUrls))

  test("strong SAS heuristic rejects absent or conflicting URLs"):
    assert(!new LilacsSasHeuristicStrong().isDuplicated(candidate, results(undefined)))
    assert(!new LilacsSasHeuristicStrong().isDuplicated(candidate, results(no)))

  test("default SAS heuristic accepts a similar URL or DOI regardless of the other results"):
    val bibliographic = Seq("title_serial", "volume_serial", "issue_number", "author", "pages")

    assert(isDuplicated(results(yes, Map("title" -> no, "title_serial" -> no, "author" -> no))))
    assert(isDuplicated(results(undefined, bibliographic.map(_ -> undefined).toMap + ("title" -> no),
      doiStatus = yes)))
    assert(isDuplicated(results(yes, doiStatus = no)))
    assert(isDuplicated(results(no, doiStatus = yes)))

  test("default SAS heuristic does not require similar journal titles"):
    assert(isDuplicated(results(undefined, Map("title_serial" -> no))))
    assert(isDuplicated(results(undefined, Map("title_serial" -> undefined))))

  test("default SAS heuristic without a similar URL or DOI requires at least five similar results"):
    // title, journal, volume, issue and author: five similar results.
    assert(isDuplicated(results(undefined, Map("pages" -> undefined))))
    // title, journal, volume and issue: four similar results.
    assert(!isDuplicated(results(undefined, Map("author" -> undefined, "pages" -> undefined))))
    assert(!isDuplicated(results(undefined, Map("volume_serial" -> undefined, "issue_number" -> undefined,
      "author" -> undefined, "pages" -> undefined))))

  test("default SAS heuristic rejects four similar results without URL and DOI"):
    // Records 7592 and 7595: title, journal, volume and author similar, issue different.
    val fourSimilar = Map("issue_number" -> no, "pages" -> undefined)

    assert(!isDuplicated(results(undefined, fourSimilar)))
    assert(isDuplicated(results(undefined, Map("issue_number" -> no))))

  test("default SAS heuristic without a similar URL or DOI allows at most one different result"):
    assert(isDuplicated(results(undefined, Map("pages" -> no))))
    assert(!isDuplicated(results(no, Map("pages" -> no))))
    assert(!isDuplicated(results(undefined, Map("pages" -> no), doiStatus = no)))

  test("default SAS heuristic without URL and DOI accepts identical titles with one difference"):
    assert(isDuplicated(results(undefined, Map("issue_number" -> no))))
    assert(isDuplicated(results(undefined)))

  test("default SAS heuristic without URL and DOI requires no difference when titles are not identical"):
    assert(isDuplicated(results(undefined, titleSimilarity = 0.9)))
    assert(!isDuplicated(results(undefined, Map("issue_number" -> no), titleSimilarity = 0.9)))

  test("default SAS heuristic with a different URL or DOI requires the URL document check"):
    assert(isDuplicated(results(yes, titleSimilarity = 0.9)))
    assert(isDuplicated(results(undefined, doiStatus = yes, titleSimilarity = 0.9)))
    // "value" is not a URL, so the content check fails without accessing the network.
    assert(!isDuplicated(results(no)))
    assert(!isDuplicated(results(undefined, doiStatus = no)))

  test("weak2 SAS heuristic requires exactly one false condition with identical titles"):
    val noFailure = results(yes)
    val oneFailure = results(no)
    val twoFailures = results(no, Map("pages" -> no))

    assert(!new LilacsSasHeuristicWeak2().isDuplicated(candidate, noFailure))
    assert(new LilacsSasHeuristicWeak2().isDuplicated(candidate, oneFailure))
    assert(!new LilacsSasHeuristicWeak2().isDuplicated(candidate, twoFailures))

  test("weak2 SAS heuristic requires exactly one false condition with similar titles"):
    val noFailure = results(yes, titleSimilarity = 0.9)
    val oneFailure = results(yes, Map("volume_serial" -> undefined), 0.9)
    val twoFailures = results(yes, Map("volume_serial" -> undefined, "author" -> no), 0.9)

    assert(!new LilacsSasHeuristicWeak2().isDuplicated(candidate, noFailure))
    assert(new LilacsSasHeuristicWeak2().isDuplicated(candidate, oneFailure))
    assert(!new LilacsSasHeuristicWeak2().isDuplicated(candidate, twoFailures))

  test("strong and weak2 SAS heuristics reject different journal titles"):
    val differentJournal = results(yes, Map("title_serial" -> no))

    assert(!new LilacsSasHeuristicStrong().isDuplicated(candidate, differentJournal))
    assert(!new LilacsSasHeuristicWeak2().isDuplicated(candidate, differentJournal))
