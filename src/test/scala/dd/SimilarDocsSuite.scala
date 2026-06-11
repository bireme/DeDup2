package dd

import dd.interfaces.{CompResult, Comparator, DocsFinder, DocsProducer, Document, Reporter}

import scala.collection.mutable.ListBuffer
import scala.util.{Success, Try}

class SimilarDocsSuite extends munit.FunSuite:
  test("SimilarDocs sends comparison results to all reporters when NGramComparator is similar"):
    val captured = ListBuffer.empty[(Document, Document, Seq[String], Seq[CompResult])]

    val finder = new DocsFinder:
      override def findDocs(searchField: String,
                            query: String,
                            auxQuery: Option[String],
                            maxDocs: Int): Try[DocsProducer] =
        Success(new DocsProducer:
          override def getDocuments: LazyList[Document] =
            LazyList(Document(Seq("id" -> "2", "title" -> query)))
        )

      override def getSearchField: Option[String] = Some("title")

      override def close(): Try[Unit] = Success(())

    val comparator = new Comparator:
      override val isGate: Boolean = true

      override def compare(originalDoc: Document,
                           currentDoc: Document): CompResult =
        CompResult(
          "NGramComparator",
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
      maxDocs = Some(10),
      otherFields = Seq("id")
    )

    val result = similarDocs.processSimilars(Document(Seq("id" -> "1", "title" -> "sample title")))

    assert(result.isSuccess)
    assertEquals(captured.size, 1)
    assertEquals(captured.head._3, Seq("id"))
    assertEquals(captured.head._4.map(_.name), Seq("NGramComparator"))
    assertEquals(captured.head._2.fields.collectFirst { case ("title", value) => value }, Some("sample title"))

  test("SimilarDocs skips reporters when NGramComparator is not similar"):
    val captured = ListBuffer.empty[(Document, Document, Seq[String], Seq[CompResult])]

    val finder = new DocsFinder:
      override def findDocs(searchField: String,
                            query: String,
                            auxQuery: Option[String],
                            maxDocs: Int): Try[DocsProducer] =
        Success(new DocsProducer:
          override def getDocuments: LazyList[Document] =
            LazyList(Document(Seq("id" -> "2", "title" -> "different title")))
        )

      override def getSearchField: Option[String] = Some("title")

      override def close(): Try[Unit] = Success(())

    val nGramComparator = new Comparator:
      override val isGate: Boolean = true

      override def compare(originalDoc: Document,
                           currentDoc: Document): CompResult =
        CompResult(
          "NGramComparator",
          "title",
          "sample title",
          "different title",
          None,
          None,
          0.25,
          isSimilar = false
        )

    val otherComparator = new Comparator:
      override def compare(originalDoc: Document,
                           currentDoc: Document): CompResult =
        CompResult(
          "ExactComparator",
          "id",
          "1",
          "2",
          None,
          None,
          1.0,
          isSimilar = true
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
      filters = Seq(nGramComparator, otherComparator),
      reporters = Seq(reporter),
      auxQuery = None,
      maxDocs = Some(10),
      otherFields = Seq("id")
    )

    val result = similarDocs.processSimilars(Document(Seq("id" -> "1", "title" -> "sample title")))

    assert(result.isSuccess)
    assertEquals(captured.size, 0)

  test("SimilarDocs does not pass maxDocs to finder when it is absent"):
    var findWithoutMaxDocsCalled = false

    val finder = new DocsFinder:
      override def findDocs(searchField: String,
                            query: String,
                            auxQuery: Option[String]): Try[DocsProducer] =
        findWithoutMaxDocsCalled = true
        Success(new DocsProducer:
          override def getDocuments: LazyList[Document] =
            LazyList(Document(Seq("id" -> "2", "title" -> query)))
        )

      override def findDocs(searchField: String,
                            query: String,
                            auxQuery: Option[String],
                            maxDocs: Int): Try[DocsProducer] =
        fail("findDocs with maxDocs should not be called when maxDocs is absent")

      override def getSearchField: Option[String] = Some("title")

      override def close(): Try[Unit] = Success(())

    val comparator = new Comparator:
      override val isGate: Boolean = true

      override def compare(originalDoc: Document,
                           currentDoc: Document): CompResult =
        CompResult("NGramComparator", "title", "sample title", "sample title", None, None, 1.0, isSimilar = true)

    val reporter = new Reporter:
      override def writeResults(originalDoc: Document,
                                currentDoc: Document,
                                otherFields: Seq[String],
                                results: Seq[CompResult]): Try[Unit] =
        Success(())

      override def close(): Try[Unit] = Success(())

    val similarDocs = new SimilarDocs(
      finder = finder,
      filters = Seq(comparator),
      reporters = Seq(reporter),
      auxQuery = None,
      maxDocs = None,
      otherFields = Seq.empty
    )

    val result = similarDocs.processSimilars(Document(Seq("id" -> "1", "title" -> "sample title")))

    assert(result.isSuccess)
    assert(findWithoutMaxDocsCalled)

  test("SimilarDocs fails when the configured search field is absent"):
    val finder = new DocsFinder:
      override def findDocs(searchField: String,
                            query: String,
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
      maxDocs = Some(5),
      otherFields = Seq.empty
    )

    val result = similarDocs.processSimilars(Document(Seq("id" -> "1")))

    assert(result.isFailure)
    assertEquals(result.failed.get.getMessage, "Empty search field")

  test("SimilarDocs requires dbase and id fields in the input schema"):
    val valid = SimilarDocs.requireSchemaFields(Map(0 -> "dbase", 1 -> "id", 2 -> "title"))
    val invalid = SimilarDocs.requireSchemaFields(Map(0 -> "dbase", 1 -> "title"))

    assert(valid.isSuccess)
    assert(invalid.isFailure)
    assertEquals(invalid.failed.get.getMessage, "Schema missing required field(s): id")

  test("SimilarDocs includes dbase and id in report fields"):
    val fields = SimilarDocs.includeRequiredReportFields(
      Seq("centro_colaborador", "id", "tipo_literatura")
    )

    assertEquals(fields, Seq("dbase", "id", "centro_colaborador", "tipo_literatura"))
