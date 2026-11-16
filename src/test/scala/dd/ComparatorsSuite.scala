package dd

import dd.comparators.{AuthorsComparator, DiceComparator, ExactComparator, JaccardComparator, LevenshteinComparator, LevenshteinJaccardComparator, NGramComparator, NumberComparator, RegexComparator, UrlsComparator}
import dd.interfaces.{Document, SimilarityStatus}

class ComparatorsSuite extends munit.FunSuite:
  test("ExactComparator normalizes values before comparing"):
    val comparator = new ExactComparator("title", normalize = true)
    val left = Document(Seq("title" -> " Sao Paulo "))
    val right = Document(Seq("title" -> "são-paulo"))

    val result = comparator.compare(left, right)

    assertEquals(result.isSimilar, SimilarityStatus.yes)
    assertEquals(result.similarity, 1.0)
    assertEquals(result.originalFieldOther, Some("saopaulo"))
    assertEquals(result.currentFieldOther, Some("saopaulo"))

  test("AuthorsComparator recognizes reordered and normalized author lists"):
    val comparator = new AuthorsComparator("authors")
    val left = Document(Seq("authors" -> "Silva, Joao; Souza, Maria"))
    val right = Document(Seq("authors" -> "Joao Silva; Maria Souza"))

    val result = comparator.compare(left, right)

    assertEquals(result.isSimilar, SimilarityStatus.yes)
    assertEquals(result.similarity, 1.0)

  test("AuthorsComparator does not split author lists on commas"):
    val comparator = new AuthorsComparator("authors")
    // One author against three exceeds ceil(1 + 1/2) = 2. Were the commas
    // separators, there would be two authors, and 2 x 3 would be accepted.
    val left = Document(Seq("authors" -> "Silva, Joao, Souza, Maria"))
    val right = Document(Seq("authors" -> "Silva, Joao; Souza, Maria; Silva, Joao Souza"))

    val result = comparator.compare(left, right)

    assertEquals(result.isSimilar, SimilarityStatus.no)
    assertEquals(result.similarity, 0.0)

  test("AuthorsComparator.compatibleSizes accepts t1 <= t2 <= ceil(t1 + t1 / 2)"):
    val maxLonger = Map(1 -> 2, 2 -> 3, 3 -> 5, 4 -> 6, 5 -> 8, 6 -> 9, 7 -> 11, 8 -> 12, 9 -> 14, 10 -> 15)

    maxLonger.foreach: (shorter, longer) =>
      assert(AuthorsComparator.compatibleSizes(shorter, shorter), s"$shorter x $shorter")
      assert(AuthorsComparator.compatibleSizes(shorter, longer), s"$shorter x $longer")
      assert(!AuthorsComparator.compatibleSizes(shorter, longer + 1), s"$shorter x ${longer + 1}")
    assert(!AuthorsComparator.compatibleSizes(4, 3))

  test("AuthorsComparator accepts a longer list with up to 50% more authors"):
    val comparator = new AuthorsComparator("authors")
    val three = "Leão, Lara Stefânia Netto de Oliveira//@//Lima, Ana Beatriz Mori//@//Oliveira, Luciana Silva da Cruz"
    val four = "Beatriz Mori Lima, Ana//@//Cristina Pimenta, Fabiana//@//Silva da Cruz Oliveira, Luciana" +
      "//@//Stefânia Netto de Oliveira Leão, Lara"
    val six = four + "//@//Souza, Pedro//@//Costa, Marcos"

    def compare(left: String, right: String) =
      comparator.compare(Document(Seq("authors" -> left)), Document(Seq("authors" -> right))).isSimilar

    assertEquals(compare(three, four), SimilarityStatus.yes)
    assertEquals(compare(four, three), SimilarityStatus.yes)
    assertEquals(compare(three, six), SimilarityStatus.no)

  test("NGramComparator reports Dice result after ngram passes threshold"):
    val comparator = new NGramComparator("title", normalize = false, minSimilarity = 0.55)
    val left = Document(Seq("title" -> "aaaaaa"))
    val right = Document(Seq("title" -> "aaaabb"))

    val result = comparator.compare(left, right)

    assertEquals(result.name, "NGramComparator")
    assertEquals(result.similarity, 0.6)
    assertEquals(result.isSimilar, SimilarityStatus.yes)

  test("UrlsComparator compares decoded URLs and reports raw and decoded values"):
    val comparator = new UrlsComparator("link")
    val encodedUrl = "http%3A%2F%2Fexample.org%2Farticle%3Fid%3D123"
    val decodedUrl = "http://example.org/article?id=123"

    val result = comparator.compare(
      Document(Seq("link" -> encodedUrl)),
      Document(Seq("link" -> decodedUrl))
    )

    assertEquals(result.isSimilar, SimilarityStatus.yes)
    assertEquals(result.similarity, 1.0)
    assertEquals(result.originalField, encodedUrl)
    assertEquals(result.currentField, decodedUrl)
    assertEquals(result.originalFieldOther, Some(decodedUrl))
    assertEquals(result.currentFieldOther, Some(decodedUrl))

  test("UrlsComparator reads pid from HTML-escaped URLs"):
    val comparator = new UrlsComparator("link")
    val scielo = "http://old.scielo.br/scielo.php?script=sci_arttext&amp;pid=S1519-698420040%s00001&amp;lng=en&amp;nrm=iso&amp;tlng=en"

    def compare(original: String, current: String) =
      comparator.compare(Document(Seq("link" -> original)), Document(Seq("link" -> current))).isSimilar

    assertEquals(compare(scielo.format("03"), scielo.format("04")), SimilarityStatus.no)
    assertEquals(compare(scielo.format("03"), scielo.format("03")), SimilarityStatus.yes)
    assertEquals(compare(scielo.format("03"), scielo.format("03").replace("&amp;", "&")), SimilarityStatus.yes)
    assertEquals(compare(scielo.format("03"), scielo.format("03").replace("&amp;", "&amp;amp;")), SimilarityStatus.yes)

  test("UrlsComparator tolerates quotes and illegal characters"):
    val comparator = new UrlsComparator("link")
    val url = "http://old.scielo.br/scielo.php?script=sci_arttext&pid=S1519-69842004000%s00001&lng=en&tlng=en"

    def compare(original: String, current: String) =
      comparator.compare(Document(Seq("link" -> original)), Document(Seq("link" -> current))).isSimilar

    assertEquals(compare(url.format("3") + "%22", url.format("4") + "%22"), SimilarityStatus.no)
    assertEquals(compare(url.format("3") + "%22", url.format("3")), SimilarityStatus.yes)
    assertEquals(compare("\"" + url.format("3") + "\"", url.format("3")), SimilarityStatus.yes)
    assertEquals(compare(url.format("3") + "&title=a b", url.format("3")), SimilarityStatus.yes)

  test("UrlsComparator.cleanUrl unescapes entities and encodes illegal characters"):
    assertEquals(UrlsComparator.cleanUrl("http://h/p?a=1&amp;b=2"), "http://h/p?a=1&b=2")
    assertEquals(UrlsComparator.cleanUrl(" \"http://h/p?a=1\" "), "http://h/p?a=1")
    assertEquals(UrlsComparator.cleanUrl("http://h/p?a=1%22"), "http://h/p?a=1")
    assertEquals(UrlsComparator.cleanUrl("http://h/p?q=a b"), "http://h/p?q=a%20b")
    assertEquals(UrlsComparator.cleanUrl("http://h/p?q=100%"), "http://h/p?q=100%")

  test("NumberComparator extracts the first numeric match"):
    val comparator = new NumberComparator("volume", regex = "\\d+")
    val result = comparator.compare(
      Document(Seq("volume" -> "vol. 012, issue 3")),
      Document(Seq("volume" -> "Volume 12"))
    )

    assertEquals(result.originalFieldOther, Some("12"))
    assertEquals(result.currentFieldOther, Some("12"))
    assertEquals(result.isSimilar, SimilarityStatus.yes)
    assertEquals(result.similarity, 1.0)

  test("NumberComparator falls back to Roman numeral conversion when no number matches"):
    val comparator = new NumberComparator("volume")
    val result = comparator.compare(
      Document(Seq("volume" -> "IX")),
      Document(Seq("volume" -> "Volume 9"))
    )

    assertEquals(result.originalFieldOther, Some("9"))
    assertEquals(result.currentFieldOther, Some("9"))
    assertEquals(result.isSimilar, SimilarityStatus.yes)

  test("NumberComparator returns no for different numbers and undefined for unparseable values"):
    val comparator = new NumberComparator("volume")
    val different = comparator.compare(
      Document(Seq("volume" -> "IX")),
      Document(Seq("volume" -> "10"))
    )
    val unparseable = comparator.compare(
      Document(Seq("volume" -> "unknown")),
      Document(Seq("volume" -> "10"))
    )

    assertEquals(different.isSimilar, SimilarityStatus.no)
    assertEquals(different.similarity, 0.0)
    assertEquals(unparseable.isSimilar, SimilarityStatus.undefined)
    assertEquals(unparseable.similarity, 0.0)

  test("comparators mark two empty fields as undefined with zero similarity"):
    val left = Document(Seq("missing" -> "", "authors" -> "", "link" -> ""))
    val right = Document(Seq("missing" -> "", "authors" -> "", "link" -> ""))
    val comparators = Seq(
      new ExactComparator("missing", normalize = false),
      new ExactComparator("missing", normalize = true),
      new DiceComparator("missing", normalize = false, minSimilarity = 0.0),
      new DiceComparator("missing", normalize = true, minSimilarity = 0.0),
      new NGramComparator("missing", normalize = false, minSimilarity = 0.0),
      new NGramComparator("missing", normalize = true, minSimilarity = 0.0),
      new RegexComparator("missing", normalize = false, regex = "\\d+", compString = "$0"),
      new RegexComparator("missing", normalize = true, regex = "\\d+", compString = "$0"),
      new LevenshteinComparator("missing", normalize = false, minSimilarity = 0.0),
      new JaccardComparator("missing", normalize = false, minSimilarity = 0.0),
      new LevenshteinJaccardComparator("missing", normalize = false, minSimilarity = 0.0),
      new AuthorsComparator("authors"),
      new UrlsComparator("link")
    )

    val results = comparators.map(_.compare(left, right))

    assert(results.forall(_.isSimilar == SimilarityStatus.undefined))
    assertEquals(results.map(_.similarity), Seq.fill(comparators.size)(0.0))

  test("comparators mark one empty field as undefined with zero similarity"):
    val left = Document(Seq("title" -> "present", "authors" -> "Silva, Joao"))
    val right = Document(Seq.empty)
    val comparators = Seq(
      new ExactComparator("title", normalize = false),
      new DiceComparator("title", normalize = false, minSimilarity = 0.0),
      new NGramComparator("title", normalize = false, minSimilarity = 0.0),
      new RegexComparator("title", normalize = false, regex = "\\d+", compString = "$0"),
      new AuthorsComparator("authors")
    )

    val results = comparators.map(_.compare(left, right))

    assert(results.forall(_.isSimilar == SimilarityStatus.undefined))
    assertEquals(results.map(_.similarity), Seq.fill(comparators.size)(0.0))
