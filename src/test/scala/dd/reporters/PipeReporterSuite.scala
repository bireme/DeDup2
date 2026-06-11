package dd.reporters

import _root_.dd.interfaces.{CompResult, Document}

import java.io.StringWriter

class PipeReporterSuite extends munit.FunSuite:
  private class FlushCountingWriter extends StringWriter:
    var flushCount: Int = 0

    override def flush(): Unit =
      flushCount += 1
      super.flush()

  test("writeResults separates header and first data row"):
    val writer = new StringWriter()
    val reporter = new PipeReporter(writer, "\n", putHeader = true, minTrue = 1)
    val originalDoc = Document(Seq("id" -> "1", "title" -> "same title"))
    val currentDoc = Document(Seq("id" -> "2", "title" -> "same title"))
    val result = CompResult(
      "NGramComparator",
      "title",
      "same title",
      "same title",
      None,
      None,
      1.0,
      isSimilar = true
    )

    val written = reporter.writeResults(originalDoc, currentDoc, Seq("id"), Seq(result))

    assert(written.isSuccess)
    assertEquals(
      writer.toString,
      "id_1|id_2|Comparator|Field|originalField|currentField|originalFieldOther|currentFieldOther|Similarity|isSimilar\n" +
        "1|2|NGramComparator|title|same title|same title|null|null|1.0|true"
    )

  test("writeResults replaces empty fields with null"):
    val writer = new StringWriter()
    val reporter = new PipeReporter(writer, "\n", putHeader = false, minTrue = 0)
    val originalDoc = Document(Seq("id" -> "", "title" -> ""))
    val currentDoc = Document(Seq("id" -> "2", "title" -> ""))
    val result = CompResult(
      "NGramComparator",
      "title",
      "",
      "",
      None,
      None,
      0.0,
      isSimilar = false
    )

    val written = reporter.writeResults(originalDoc, currentDoc, Seq("id"), Seq(result))

    assert(written.isSuccess)
    assertEquals(
      writer.toString,
      "null|2|NGramComparator|title|null|null|null|null|0.0|false"
    )

  test("writeResults exports original and other comparison fields separately"):
    val writer = new StringWriter()
    val reporter = new PipeReporter(writer, "\n", putHeader = false, minTrue = 1)
    val result = CompResult(
      "NGramComparator",
      "title",
      "São Paulo",
      "Sao Paulo",
      Some("saopaulo"),
      Some("saopaulo"),
      1.0,
      isSimilar = true
    )

    val written = reporter.writeResults(
      Document(Seq("title" -> "São Paulo")),
      Document(Seq("title" -> "Sao Paulo")),
      Seq.empty,
      Seq(result)
    )

    assert(written.isSuccess)
    assertEquals(
      writer.toString,
      "NGramComparator|title|São Paulo|Sao Paulo|saopaulo|saopaulo|1.0|true"
    )

  test("writeResults keeps one output column for each other field side"):
    val writer = new StringWriter()
    val reporter = new PipeReporter(writer, "\n", putHeader = true, minTrue = 1)
    val originalDoc = Document(Seq("id" -> "1", "title" -> "same title"))
    val currentDoc = Document(Seq("title" -> "same title"))
    val result = CompResult(
      "NGramComparator",
      "title",
      "same title",
      "same title",
      None,
      None,
      1.0,
      isSimilar = true
    )

    val written = reporter.writeResults(originalDoc, currentDoc, Seq("id", "missing"), Seq(result))

    assert(written.isSuccess)
    assertEquals(
      writer.toString,
      "id_1|id_2|missing_1|missing_2|Comparator|Field|originalField|currentField|originalFieldOther|currentFieldOther|Similarity|isSimilar\n" +
        "1|null|null|null|NGramComparator|title|same title|same title|null|null|1.0|true"
    )

  test("writeResults flushes immediately when flushResults is enabled"):
    val writer = new FlushCountingWriter()
    val reporter = new PipeReporter(writer, "\n", putHeader = false, minTrue = 1, flushResults = true)
    val result = CompResult(
      "ExactComparator",
      "title",
      "same",
      "same",
      None,
      None,
      1.0,
      isSimilar = true
    )

    val written = reporter.writeResults(
      Document(Seq("id" -> "1")),
      Document(Seq("id" -> "2")),
      Seq("id"),
      Seq(result)
    )

    assert(written.isSuccess)
    assertEquals(writer.flushCount, 1)
