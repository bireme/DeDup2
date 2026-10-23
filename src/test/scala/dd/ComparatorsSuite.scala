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
    val left = Document(Seq("authors" -> "Silva, Joao, Souza, Maria"))
    val right = Document(Seq("authors" -> "Silva, Joao; Souza, Maria"))

    val result = comparator.compare(left, right)

    assertEquals(result.isSimilar, SimilarityStatus.no)
    assertEquals(result.similarity, 0.0)

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
