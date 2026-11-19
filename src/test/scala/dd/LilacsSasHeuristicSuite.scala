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

  /** Bibliographic fields other than the title. */
  private val bibliographic: Seq[String] = Seq("title_serial", "volume_serial", "issue_number", "author", "pages")

  /** Similarity of the titles of records 41320 and 41321 ("I. Parte" and "II parte"). */
  private val partTitleSimilarity: Double = 80.0 / 81.0

  test("default SAS heuristic accepts a similar URL or DOI regardless of the other results"):
    assert(isDuplicated(results(yes, Map("title" -> no, "title_serial" -> no, "author" -> no))))
    assert(isDuplicated(results(undefined, bibliographic.map(_ -> undefined).toMap + ("title" -> no),
      doiStatus = yes)))
    assert(isDuplicated(results(yes, doiStatus = no)))
    assert(isDuplicated(results(no, doiStatus = yes)))

  test("default SAS heuristic does not require similar journal titles"):
    assert(isDuplicated(results(undefined, Map("title_serial" -> no))))
    assert(isDuplicated(results(undefined, Map("title_serial" -> undefined))))

  test("default SAS heuristic without a similar URL or DOI allows at most one different result"):
    assert(isDuplicated(results(undefined, Map("issue_number" -> no))))
    assert(!isDuplicated(results(undefined, Map("issue_number" -> no, "author" -> no))))
    assert(!isDuplicated(results(undefined, Map("pages" -> no), doiStatus = no)))
    assert(!isDuplicated(results(no, Map("pages" -> no))))

  test("identical titles without URL and DOI: no difference accepts empty bibliographic fields"):
    assert(isDuplicated(results(undefined)))
    assert(isDuplicated(results(undefined, Map("pages" -> undefined))))
    // Only the title is filled: there is no minimum number of similar results.
    assert(isDuplicated(results(undefined, bibliographic.map(_ -> undefined).toMap)))

  test("identical titles without URL and DOI: one difference requires every bibliographic field and similar pages"):
    assert(isDuplicated(results(undefined, Map("issue_number" -> no))))
    // The only difference is in the pages.
    assert(!isDuplicated(results(undefined, Map("pages" -> no))))
    // Records 7592 and 7595: issue different and pages empty.
    assert(!isDuplicated(results(undefined, Map("issue_number" -> no, "pages" -> undefined))))
    assert(!isDuplicated(results(undefined, Map("issue_number" -> no, "author" -> undefined))))

  test("similar titles without URL and DOI: no difference, at most one empty field and similar pages"):
    assert(isDuplicated(results(undefined, titleSimilarity = 0.9)))
    assert(isDuplicated(results(undefined, Map("author" -> undefined), titleSimilarity = 0.9)))
    assert(isDuplicated(results(undefined, Map("title_serial" -> undefined), titleSimilarity = 0.9)))
    assert(!isDuplicated(results(undefined, Map("author" -> undefined, "volume_serial" -> undefined),
      titleSimilarity = 0.9)))
    assert(!isDuplicated(results(undefined, Map("issue_number" -> no), titleSimilarity = 0.9)))
    assert(!isDuplicated(results(undefined, Map("pages" -> no), titleSimilarity = 0.9)))

  test("similar titles without URL and DOI: empty pages are rejected (records 41320 and 41321)"):
    assert(!isDuplicated(results(undefined, Map("pages" -> undefined), titleSimilarity = partTitleSimilarity)))
    assert(isDuplicated(results(undefined, titleSimilarity = partTitleSimilarity)))

  /** Removes the URL and DOI results, as in a configuration without those comparators. */
  private def withoutUrlAndDoi(results: Seq[CompResult]): Seq[CompResult] =
    results.filterNot(result => result.fieldName == "electronic_address" || result.fieldName == "doi_number")

  test("without URL and DOI comparators, empty fields are counted the same way"):
    // Identical titles, one difference, two empty fields: was accepted when the
    // count included the missing URL and DOI comparators.
    assert(!isDuplicated(withoutUrlAndDoi(results(undefined,
      Map("issue_number" -> no, "author" -> undefined, "pages" -> undefined)))))
    assert(isDuplicated(withoutUrlAndDoi(results(undefined, Map("issue_number" -> no)))))
    // Similar titles: at most one empty field and similar pages.
    assert(isDuplicated(withoutUrlAndDoi(results(undefined, Map("author" -> undefined), titleSimilarity = 0.9))))
    assert(!isDuplicated(withoutUrlAndDoi(results(undefined,
      Map("author" -> undefined, "volume_serial" -> undefined), titleSimilarity = 0.9))))
    assert(!isDuplicated(withoutUrlAndDoi(results(undefined, Map("pages" -> undefined),
      titleSimilarity = partTitleSimilarity))))

  test("with or without URL and DOI comparators, undefined URL and DOI give the same decision"):
    val cases: Seq[(Map[String, SimilarityStatus], Double)] = Seq(
      (Map.empty, 1.0),
      (Map("issue_number" -> no), 1.0),
      (Map("issue_number" -> no, "pages" -> undefined), 1.0),
      (bibliographic.map(_ -> undefined).toMap, 1.0),
      (Map("pages" -> no, "author" -> no), 1.0),
      (Map.empty, 0.9),
      (Map("author" -> undefined), 0.9),
      (Map("author" -> undefined, "volume_serial" -> undefined), 0.9),
      (Map("pages" -> undefined), partTitleSimilarity),
      (Map("issue_number" -> no), 0.9))

    cases.foreach: (overrides, titleSimilarity) =>
      val withComparators: Seq[CompResult] = results(undefined, overrides, titleSimilarity)
      assertEquals(isDuplicated(withoutUrlAndDoi(withComparators)), isDuplicated(withComparators),
        s"$overrides, title similarity $titleSimilarity")

  test("default SAS heuristic with a different URL or DOI requires the same final URL"):
    // "value" is not a URL, so the final URL check fails without accessing the network.
    assert(!isDuplicated(results(no)))
    assert(!isDuplicated(results(no, bibliographic.map(_ -> undefined).toMap)))
    // An undefined URL never has the same final URL.
    assert(!isDuplicated(results(undefined, doiStatus = no)))
    // Different URL and DOI: two differences.
    assert(!isDuplicated(results(no, doiStatus = no)))

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
