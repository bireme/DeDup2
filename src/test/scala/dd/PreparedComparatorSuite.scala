package dd

import dd.comparators.*
import dd.interfaces.{CompResult, Comparator, DocsFinder, DocsProducer, Document, PreparedComparator, Reporter, SimilarityStatus}

import java.util.concurrent.atomic.AtomicInteger
import scala.util.{Random, Success, Try}

/**
 * Checks that preparing a comparator for a source document returns exactly the
 * same results as comparing the documents directly, and that the pipeline
 * prepares each source document only once.
 */
class PreparedComparatorSuite extends munit.FunSuite:
  private val stringComparators: Seq[Comparator] =
    for
      normalize <- Seq(true, false)
      comparator <- Seq[Comparator](
        DiceComparator("title", normalize, 0.7),
        LevenshteinComparator("title", normalize, 0.8),
        JaccardComparator("title", normalize, 0.5),
        LevenshteinJaccardComparator("title", normalize, 0.75)
      )
    yield comparator

  private val samples: Seq[Document] = Seq(
    Document(Seq()),
    Document(Seq("title" -> "")),
    Document(Seq("title" -> "   ")),
    Document(Seq("title" -> "a")),
    Document(Seq("title" -> "b")),
    Document(Seq("title" -> "!!")),
    Document(Seq("title" -> " São Paulo ")),
    Document(Seq("title" -> "sao-paulo")),
    Document(Seq("title" -> "Saúde pública no Brasil")),
    Document(Seq("title" -> "Saude publica no Brasil", "other" -> "x")),
    Document(Seq("title" -> "Saúde", "title" -> "pública no Brasil")),
    Document(Seq("other" -> "Saúde pública no Brasil"))
  )

  test("DiceComparator keeps its previous results"):
    val comparator = DiceComparator("title", normalize = true, 0.7)

    val similar = comparator.compare(Document(Seq("title" -> " Night ")), Document(Seq("title" -> "nacht")))
    assertEquals(similar, CompResult("DiceComparator", "title", "Night", "nacht",
      Some("night"), Some("nacht"), 0.25, SimilarityStatus.no))

    val equal = comparator.compare(Document(Seq("title" -> "São Paulo")), Document(Seq("title" -> "sao paulo")))
    assertEquals(equal.similarity, 1.0)
    assertEquals(equal.isSimilar, SimilarityStatus.yes)

    val undefined = comparator.compare(Document(Seq("title" -> "abc")), Document(Seq("other" -> "abc")))
    assertEquals(undefined, CompResult("DiceComparator", "title", "abc", "",
      Some("abc"), Some(""), 0.0, SimilarityStatus.undefined))

  test("prepared comparators return the same results as compare for every sample pair"):
    for
      comparator <- stringComparators
      original <- samples
      prepared = comparator.prepare(original)
      current <- samples
    do
      assertEquals(prepared.compare(current), comparator.compare(original, current),
        s"${comparator.getClass.getSimpleName} differs for $original x $current")

  test("prepared comparators return the same results as compare for random documents"):
    val random: Random = new Random(20261016L)
    val alphabet: IndexedSeq[Char] = "aábcçeéo ,.-".toIndexedSeq
    def randomDocument(): Document =
      Document(Seq.fill(random.nextInt(3)):
        "title" -> String.valueOf(Array.fill(random.nextInt(12))(alphabet(random.nextInt(alphabet.length)))))

    for _ <- 1 to 300 do
      val original: Document = randomDocument()
      val candidates: Seq[Document] = Seq.fill(10)(randomDocument())
      stringComparators.foreach: comparator =>
        val prepared: PreparedComparator = comparator.prepare(original)
        candidates.foreach: current =>
          assertEquals(prepared.compare(current), comparator.compare(original, current))

  test("AuthorsComparator matches authors across separators, order and accents"):
    val comparator = AuthorsComparator("author")
    val result = comparator.compare(
      Document(Seq("author" -> "Silva, João//@//Souza, Maria")),
      Document(Seq("author" -> "Maria Souza; Joao Silva"))
    )
    assertEquals(result, CompResult("AuthorsComparator", "author", "Silva, João//@//Souza, Maria",
      "Maria Souza//@//Joao Silva", None, None, 1.0, SimilarityStatus.yes))

  test("AuthorsComparator requires author lists of comparable sizes"):
    val comparator = AuthorsComparator("author")
    val five: Document = Document(Seq("author" -> "Silva, A//@//Souza, B//@//Lima, C//@//Costa, D//@//Rocha, E"))

    val oneOfFive = comparator.compare(Document(Seq("author" -> "Silva, A")), five)
    assertEquals(oneOfFive.isSimilar, SimilarityStatus.no)

    val fourOfFive = comparator.compare(Document(Seq("author" -> "Silva, A//@//Souza, B//@//Lima, C//@//Costa, D")), five)
    assertEquals(fourOfFive.isSimilar, SimilarityStatus.yes)

    val missing = comparator.compare(Document(Seq("author" -> "  ")), five)
    assertEquals(missing.isSimilar, SimilarityStatus.undefined)

  test("a prepared AuthorsComparator can be reused for many candidates"):
    val comparator = AuthorsComparator("author")
    val authors: Seq[Document] = Seq(
      "Silva, João//@//Souza, Maria", "Joao Silva; Maria Souza", "SILVA, J.//@//SOUZA, M.",
      "Costa, Pedro", "", "Silva, João//@//Souza, Maria//@//Lima, Ana"
    ).map(value => Document(Seq("author" -> value)))
    for original <- authors do
      val prepared: PreparedComparator = comparator.prepare(original)
      for current <- authors ++ authors do
        assertEquals(prepared.compare(current), comparator.compare(original, current))

  test("comparators without their own prepare delegate to compare"):
    val comparators: Seq[Comparator] = Seq(
      ExactComparator("title", normalize = true),
      NGramComparator("title", normalize = true, 0.5),
      RegexComparator("title", normalize = true, "a", "b")
    )
    for comparator <- comparators; original <- samples; current <- samples do
      assertEquals(comparator.prepare(original).compare(current), comparator.compare(original, current))

    val urls: Seq[Document] = Seq(
      Document(Seq("link" -> "http://www.bireme.br/a?x=1")),
      Document(Seq("link" -> "https://www.bireme.br/a?x=1")),
      Document(Seq("link" -> "http://example.org/b"))
    )
    val urlsComparator = UrlsComparator("link")
    for original <- urls; current <- urls do
      assertEquals(urlsComparator.prepare(original).compare(current), urlsComparator.compare(original, current))

  test("SimilarDocs prepares each source document once and compares every candidate"):
    val prepareCalls: AtomicInteger = AtomicInteger(0)
    val compareCalls: AtomicInteger = AtomicInteger(0)
    val comparator = new Comparator:
      override def compare(originalDoc: Document, currentDoc: Document): CompResult =
        CompResult("CountingComparator", "title", "", "", None, None, 1.0, SimilarityStatus.yes)

      override def prepare(originalDoc: Document): PreparedComparator =
        prepareCalls.incrementAndGet()
        currentDoc =>
          compareCalls.incrementAndGet()
          compare(originalDoc, currentDoc)

    val finder = new DocsFinder:
      override def findDocs(searchField: String,
                            query: String,
                            auxQuery: Option[String],
                            maxDocs: Int): Try[DocsProducer] =
        Success(new DocsProducer:
          override def getDocuments: Iterator[Document] =
            Iterator.tabulate(5)(index => Document(Seq("id" -> index.toString, "title" -> query))))
      override def getSearchField: Option[String] = Some("title")
      override def getMinSimilarity: Option[Double] = None
      override def close(): Try[Unit] = Success(())

    val reporter = new Reporter:
      override def writeResults(originalDoc: Document, currentDoc: Document,
                                otherFields: Seq[String], results: Seq[CompResult]): Try[Unit] = Success(())
      override def close(): Try[Unit] = Success(())

    val similarDocs = new SimilarDocs(finder, Seq(comparator), Seq(reporter), None, Some(10), Seq())
    assert(similarDocs.processSimilars(Document(Seq("id" -> "a", "title" -> "first"))).isSuccess)
    assert(similarDocs.processSimilars(Document(Seq("id" -> "b", "title" -> "second"))).isSuccess)

    assertEquals(prepareCalls.get, 2)
    assertEquals(compareCalls.get, 10)
