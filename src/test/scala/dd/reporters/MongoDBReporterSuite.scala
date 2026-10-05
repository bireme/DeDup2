package dd.reporters

import _root_.dd.interfaces.{CompResult, SimilarityStatus}

class MongoDBReporterSuite extends munit.FunSuite:
  test("resultFieldFor exports CompResult nested by field name"):
    val result = CompResult(
      "NGramComparator",
      "title",
      "Original Title",
      "Current Title",
      Some("originaltitle"),
      Some("currenttitle"),
      0.75,
      isSimilar = SimilarityStatus.yes
    )

    val (fieldName, document) = MongoDBReporter.resultFieldFor(result)

    assertEquals(fieldName, "title")
    assertEquals(document.getString("name"), "NGramComparator")
    assertEquals(document.getString("originalField"), "Original Title")
    assertEquals(document.getString("currentField"), "Current Title")
    assertEquals(document.getString("originalFieldOther"), "originaltitle")
    assertEquals(document.getString("currentFieldOther"), "currenttitle")
    assertEquals(document.getDouble("similarity").doubleValue(), 0.75)
    assertEquals(document.getString("isSimilar"), "yes")

  test("resultFieldFor keeps the configured field name when compared fields are empty"):
    val result = CompResult(
      "NGramComparator",
      "title",
      "",
      "",
      None,
      None,
      1.0,
      isSimilar = SimilarityStatus.yes
    )

    val (fieldName, document) = MongoDBReporter.resultFieldFor(result)

    assertEquals(fieldName, "title")
    assertEquals(document.getString("originalField"), "")
    assertEquals(document.getString("currentField"), "")
    assertEquals(document.getString("isSimilar"), "yes")

    val (fieldNameWithOnlyCurrentEmpty, _) = MongoDBReporter.resultFieldFor(
      result.copy(originalField = "Original Title")
    )

    assertEquals(fieldNameWithOnlyCurrentEmpty, "title")

  test("shouldWriteResults applies minTrue threshold"):
    val similarResult = CompResult("NGramComparator", "title", "a", "a", None, None, 1.0, isSimilar = SimilarityStatus.yes)
    val differentResult = CompResult("ExactComparator", "year", "2020", "2021", None, None, 0.0, isSimilar = SimilarityStatus.no)

    assertEquals(MongoDBReporter.shouldWriteResults(Seq(similarResult, differentResult), minTrue = 0), true)
    assertEquals(MongoDBReporter.shouldWriteResults(Seq(similarResult, differentResult), minTrue = 1), true)
    assertEquals(MongoDBReporter.shouldWriteResults(Seq(similarResult, differentResult), minTrue = 2), false)
