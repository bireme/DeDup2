package dd.reporters

import _root_.dd.interfaces.{CompResult, Document, SimilarityStatus}
import play.api.libs.json.{JsArray, JsNull, Json}

import java.io.StringWriter

class JsonReporterSuite extends munit.FunSuite:
  private class FlushCountingWriter extends StringWriter:
    var flushCount: Int = 0

    override def flush(): Unit =
      flushCount += 1
      super.flush()

  test("writeResults writes valid JSON with fields and results"):
    val writer = new StringWriter()
    val reporter = new JsonReporter(writer, minTrue = 1)
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
      isSimilar = SimilarityStatus.yes
    )

    val written = reporter.writeResults(originalDoc, currentDoc, Seq("id"), Seq(result))
    val closed = reporter.close()
    val json = Json.parse(writer.toString)

    assert(written.isSuccess)
    assert(closed.isSuccess)
    assertEquals((json \ 0 \ "fields" \ "id" \ "original").as[String], "1")
    assertEquals((json \ 0 \ "fields" \ "id" \ "current").as[String], "2")
    assertEquals((json \ 0 \ "results" \ 0 \ "comparator").as[String], "NGramComparator")
    assertEquals((json \ 0 \ "results" \ 0 \ "field").as[String], "title")
    assertEquals((json \ 0 \ "results" \ 0 \ "similarity").as[Double], 1.0)
    assertEquals((json \ 0 \ "results" \ 0 \ "isSimilar").as[String], "yes")

  test("writeResults serializes empty values as JSON null"):
    val writer = new StringWriter()
    val reporter = new JsonReporter(writer, minTrue = 0)
    val originalDoc = Document(Seq("id" -> "", "title" -> ""))
    val currentDoc = Document(Seq("id" -> "2", "title" -> ""))
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

    val written = reporter.writeResults(originalDoc, currentDoc, Seq("id"), Seq(result))
    reporter.close()
    val json = Json.parse(writer.toString)

    assert(written.isSuccess)
    assertEquals((json \ 0 \ "fields" \ "id" \ "original").get, JsNull)
    assertEquals((json \ 0 \ "fields" \ "id" \ "current").as[String], "2")
    assertEquals((json \ 0 \ "results" \ 0 \ "originalField").get, JsNull)
    assertEquals((json \ 0 \ "results" \ 0 \ "currentFieldOther").get, JsNull)
    assertEquals((json \ 0 \ "results" \ 0 \ "similarity").as[Double], 1.0)
    assertEquals((json \ 0 \ "results" \ 0 \ "isSimilar").as[String], "yes")

  test("writeResults skips rows below minTrue and closes as empty array"):
    val writer = new StringWriter()
    val reporter = new JsonReporter(writer, minTrue = 1)
    val result = CompResult(
      "ExactComparator",
      "title",
      "left",
      "right",
      None,
      None,
      0.0,
      isSimilar = SimilarityStatus.no
    )

    val written = reporter.writeResults(
      Document(Seq("id" -> "1")),
      Document(Seq("id" -> "2")),
      Seq("id"),
      Seq(result)
    )
    reporter.close()

    assert(written.isSuccess)
    assertEquals(Json.parse(writer.toString), JsArray.empty)

  test("writeResults flushes immediately when flushResults is enabled"):
    val writer = new FlushCountingWriter()
    val reporter = new JsonReporter(writer, minTrue = 1, flushResults = true)
    val result = CompResult(
      "ExactComparator",
      "title",
      "same",
      "same",
      None,
      None,
      1.0,
      isSimilar = SimilarityStatus.yes
    )

    val written = reporter.writeResults(
      Document(Seq("id" -> "1")),
      Document(Seq("id" -> "2")),
      Seq("id"),
      Seq(result)
    )

    assert(written.isSuccess)
    assertEquals(writer.flushCount, 1)

  test("writeResults pretty prints JSON when prettyPrint is enabled"):
    val writer = new StringWriter()
    val reporter = new JsonReporter(writer, minTrue = 1, prettyPrint = true)
    val result = CompResult("ExactComparator", "title", "same", "same", None, None, 1.0, isSimilar = SimilarityStatus.yes)

    reporter.writeResults(
      Document(Seq("id" -> "1")),
      Document(Seq("id" -> "2")),
      Seq("id"),
      Seq(result)
    )
    reporter.close()

    val output: String = writer.toString
    assert(output.contains("\n  \"fields\""))
    assert(Json.parse(output).as[JsArray].value.nonEmpty)
