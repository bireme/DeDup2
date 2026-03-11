package dd

import dd.interfaces.{CompResult, Comparator, DocsFinder, DocsProducer, Document, Reporter}

import scala.collection.mutable.ListBuffer
import scala.util.{Success, Try}

class SimilarDocsSuite extends munit.FunSuite:
  test("SimilarDocs sends comparison results to all reporters"):
    val captured = ListBuffer.empty[(Document, Document, Seq[String], Seq[CompResult])]

    val finder = new DocsFinder:
      override def findDocs(query: String,
                            auxQuery: Option[String],
                            maxDocs: Int): Try[DocsProducer] =
        Success(new DocsProducer:
          override def getDocuments: LazyList[Document] =
            LazyList(Document(Seq("id" -> "2", "title" -> query)))
        )

      override def getSearchField: Option[String] = Some("title")

      override def close(): Try[Unit] = Success(())

    val comparator = new Comparator:
      override def compare(originalDoc: Document,
                           currentDoc: Document): CompResult =
        CompResult(
          "StubComparator",
          "title",
          originalDoc.fields.collectFirst { case ("title", value) => value }.getOrElse(""),
          currentDoc.fields.collectFirst { case ("title", value) => value }.getOrElse(""),
          None,
          None,
          1.0,
          true
        )

    val reporter = new Reporter:
      override def writeResults(originalDoc: Document,
                                currentDoc: Document,
                                otherFields: Seq[String],
                                results: Seq[CompResult]): Try[Unit] =
        captured += ((originalDoc, currentDoc, otherFields, results))
        Success(())

      override def close(): Try[Unit] = Success(())

    val similarDocs = new SimilarDocs(
      finder = finder,
      filters = Seq(comparator),
      reporters = Seq(reporter),
      auxQuery = Some("status:active"),
      maxDocs = 10,
      otherFields = Seq("id")
    )

    val result = similarDocs.processSimilars(Document(Seq("id" -> "1", "title" -> "sample title")))

    assert(result.isSuccess)
    assertEquals(captured.size, 1)
    assertEquals(captured.head._3, Seq("id"))
    assertEquals(captured.head._4.map(_.name), Seq("StubComparator"))
    assertEquals(captured.head._2.fields.collectFirst { case ("title", value) => value }, Some("sample title"))

  test("SimilarDocs fails when the configured search field is absent"):
    val finder = new DocsFinder:
      override def findDocs(query: String,
                            auxQuery: Option[String],
                            maxDocs: Int): Try[DocsProducer] =
        fail("findDocs should not be called when the search field is missing from the document")

      override def getSearchField: Option[String] = Some("title")

      override def close(): Try[Unit] = Success(())

    val similarDocs = new SimilarDocs(
      finder = finder,
      filters = Seq.empty,
      reporters = Seq.empty,
      auxQuery = None,
      maxDocs = 5,
      otherFields = Seq.empty
    )

    val result = similarDocs.processSimilars(Document(Seq("id" -> "1")))

    assert(result.isFailure)
    assertEquals(result.failed.get.getMessage, "Empty search field")
