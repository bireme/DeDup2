package dd

import dd.configurators.ConfMain.SimilarDocsConfig
import dd.interfaces.{CompResult, Comparator, DocsFinder, DocsProducer, Document, Reporter, SimilarityStatus}

import java.util.concurrent.atomic.AtomicInteger
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

      override def getMinSimilarity: Option[Double] = Some(0.8)

      override def close(): Try[Unit] = Success(())

    val comparator = new Comparator:
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
          SimilarityStatus.yes
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
    assertEquals(captured.head._4.map(_.name), Seq("DiceComparator", "NGramComparator"))
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

      override def getMinSimilarity: Option[Double] = Some(0.8)

      override def close(): Try[Unit] = Success(())

    val nGramComparator = new Comparator:
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
          isSimilar = SimilarityStatus.no
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
          isSimilar = SimilarityStatus.yes
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
    assertEquals(captured.size, 1)
    assertEquals(captured.head._4.map(_.name), Seq("DiceComparator", "NGramComparator", "ExactComparator"))

  test("SimilarDocs sends comparison results when no gate comparator is configured"):
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

      override def getMinSimilarity: Option[Double] = Some(0.8)

      override def close(): Try[Unit] = Success(())

    val exactComparator = new Comparator:
      override def compare(originalDoc: Document,
                           currentDoc: Document): CompResult =
        CompResult(
          "ExactComparator",
          "title",
          "sample title",
          "sample title",
          None,
          None,
          1.0,
          isSimilar = SimilarityStatus.yes
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
      filters = Seq(exactComparator),
      reporters = Seq(reporter),
      auxQuery = None,
      maxDocs = Some(10),
      otherFields = Seq("id")
    )

    val result = similarDocs.processSimilars(Document(Seq("id" -> "1", "title" -> "sample title")))

    assert(result.isSuccess)
    assertEquals(captured.size, 1)
    assertEquals(captured.head._4.map(_.name), Seq("DiceComparator", "ExactComparator"))

  test("SimilarDocs uses default maxDocs when it is absent"):
    var receivedMaxDocs = Option.empty[Int]

    val finder = new DocsFinder:
      override def findDocs(searchField: String,
                            query: String,
                            auxQuery: Option[String],
                            maxDocs: Int): Try[DocsProducer] =
        receivedMaxDocs = Some(maxDocs)
        Success(new DocsProducer:
          override def getDocuments: LazyList[Document] =
            LazyList(Document(Seq("id" -> "2", "title" -> query)))
        )

      override def getSearchField: Option[String] = Some("title")

      override def getMinSimilarity: Option[Double] = Some(0.8)

      override def close(): Try[Unit] = Success(())

    val comparator = new Comparator:
      override def compare(originalDoc: Document,
                           currentDoc: Document): CompResult =
        CompResult("NGramComparator", "title", "sample title", "sample title", None, None, 1.0, isSimilar = SimilarityStatus.yes)

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
    assertEquals(receivedMaxDocs, Some(1000))

  test("SimilarDocs fails when the configured search field is absent"):
    val finder = new DocsFinder:
      override def findDocs(searchField: String,
                            query: String,
                            auxQuery: Option[String],
                            maxDocs: Int): Try[DocsProducer] =
        fail("findDocs should not be called when the search field is missing from the document")

      override def getSearchField: Option[String] = Some("title")

      override def getMinSimilarity: Option[Double] = Some(0.8)

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

  test("SimilarDocs runs comparators for the same candidate document in parallel"):
    val running = AtomicInteger(0)
    val maxRunning = AtomicInteger(0)

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

      override def getMinSimilarity: Option[Double] = None

      override def close(): Try[Unit] = Success(())

    def blockingComparator(name: String): Comparator =
      new Comparator:
        override def compare(originalDoc: Document,
                             currentDoc: Document): CompResult =
          val currentRunning = running.incrementAndGet()
          maxRunning.accumulateAndGet(currentRunning, Math.max)
          try
            Thread.sleep(100)
            CompResult(name, "title", "sample title", "sample title", None, None, 1.0, isSimilar = SimilarityStatus.yes)
          finally running.decrementAndGet()

    val reporter = new Reporter:
      override def writeResults(originalDoc: Document,
                                currentDoc: Document,
                                otherFields: Seq[String],
                                results: Seq[CompResult]): Try[Unit] =
        assertEquals(results.map(_.name), Seq("FirstComparator", "SecondComparator"))
        Success(())

      override def close(): Try[Unit] = Success(())

    val similarDocs = new SimilarDocs(
      finder = finder,
      filters = Seq(blockingComparator("FirstComparator"), blockingComparator("SecondComparator")),
      reporters = Seq(reporter),
      auxQuery = None,
      maxDocs = Some(10),
      otherFields = Seq.empty
    )

    assert(similarDocs.processSimilars(Document(Seq("id" -> "1", "title" -> "sample title"))).isSuccess)
    assertEquals(maxRunning.get(), 2)

  test("SimilarDocs processes documents with a continuous fixed-size worker pool"):
    val running = AtomicInteger(0)
    val maxRunning = AtomicInteger(0)
    val events = ListBuffer.empty[String]

    def record(event: String): Unit = events.synchronized(events += event)

    val finder = new DocsFinder:
      override def findDocs(searchField: String,
                            query: String,
                            auxQuery: Option[String],
                            maxDocs: Int): Try[DocsProducer] =
        val currentRunning = running.incrementAndGet()
        maxRunning.accumulateAndGet(currentRunning, Math.max)
        record(s"start-$query")
        try
          val delay = query match
            case "1" => 250L
            case "2" => 80L
            case _ => 20L
          Thread.sleep(delay)
          record(s"finish-$query")
          Success(new DocsProducer:
            override def getDocuments: LazyList[Document] = LazyList.empty
          )
        finally running.decrementAndGet()

      override def getSearchField: Option[String] = Some("title")

      override def getMinSimilarity: Option[Double] = None

      override def close(): Try[Unit] = Success(())

    val similarDocs = new SimilarDocs(
      SimilarDocsConfig(
        producer = new DocsProducer:
          override def getDocuments: LazyList[Document] =
            LazyList("1", "2", "3", "4").map(id => Document(Seq("id" -> id, "title" -> id))),
        finder = finder,
        comparators = Seq.empty,
        reporters = Seq.empty,
        auxQuery = None,
        maxDocs = Some(10),
        documentParallelism = 2
      )
    )

    assert(similarDocs.run().isSuccess)

    val snapshot = events.synchronized(events.toList)
    assertEquals(maxRunning.get(), 2)
    assert(snapshot.indexOf("start-3") < snapshot.indexOf("finish-1"))

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

  test("SimilarDocs prepends DiceComparator for indexed field when absent"):
    val finder = new DocsFinder:
      override def findDocs(searchField: String,
                            query: String,
                            auxQuery: Option[String],
                            maxDocs: Int): Try[DocsProducer] =
        fail("finder should not be used by this unit test")

      override def getSearchField: Option[String] = Some("title")

      override def getMinSimilarity: Option[Double] = Some(0.8)

      override def close(): Try[Unit] = Success(())

    val comparator = new Comparator:
      override def compare(originalDoc: Document,
                           currentDoc: Document): CompResult =
        CompResult("ExactComparator", "publication_year", "2024", "2024", None, None, 1.0, isSimilar = SimilarityStatus.yes)

    val comparators = SimilarDocs.includeIndexedFieldDiceComparator(finder, Seq(comparator))
    val result = comparators.head.compare(
      Document(Seq("title" -> "Saude publica")),
      Document(Seq("title" -> "Saúde pública"))
    )

    assertEquals(comparators.size, 2)
    assertEquals(result.name, "DiceComparator")
    assertEquals(result.fieldName, "title")
    assertEquals(result.isSimilar, SimilarityStatus.yes)

  test("SimilarDocs does not duplicate DiceComparator for indexed field"):
    val finder = new DocsFinder:
      override def findDocs(searchField: String,
                            query: String,
                            auxQuery: Option[String],
                            maxDocs: Int): Try[DocsProducer] =
        fail("finder should not be used by this unit test")

      override def getSearchField: Option[String] = Some("title")

      override def getMinSimilarity: Option[Double] = Some(0.8)

      override def close(): Try[Unit] = Success(())

    val dice = dd.comparators.DiceComparator("title", normalize = true, minSimilarity = 0.8)
    val comparators = SimilarDocs.includeIndexedFieldDiceComparator(finder, Seq(dice))

    assertEquals(comparators, Seq(dice))
