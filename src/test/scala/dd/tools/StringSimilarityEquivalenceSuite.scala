package dd.tools

import dd.tools.StringSimilarity.{DiceCoefficient, Levenshtein}
import org.apache.commons.csv.CSVFormat

import java.io.FileReader
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters.*
import scala.util.{Random, Using}

/**
 * Checks that the optimized Dice and Levenshtein implementations return exactly
 * the same results as the original (allocation-heavy) implementations.
 */
class StringSimilarityEquivalenceSuite extends munit.FunSuite:

  /** Original implementations, kept verbatim as the reference behaviour. */
  object Reference:
    def dice(left: String, right: String): Double =
      diceCoefficient(left.toCharArray, right.toCharArray)

    private def diceCoefficient[T](left: Array[T], right: Array[T]): Double =
      val leftPairs: Set[(T, T)] =
        if left.length < 2 then Set.empty[(T, T)]
        else left.zip(left.tail).toSet

      val rightPairs: Set[(T, T)] =
        if right.length < 2 then Set.empty[(T, T)]
        else right.zip(right.tail).toSet

      if leftPairs.isEmpty && rightPairs.isEmpty then
        if left.sameElements(right) then 1.0 else 0.0
      else
        val intersection: Set[(T, T)] = leftPairs intersect rightPairs
        2.0 * intersection.size / (leftPairs.size + rightPairs.size)

    def levenshtein(left: String, right: String): Int =
      val first: Array[Char] = left.toCharArray
      val second: Array[Char] = right.toCharArray
      var previous: Array[Int] = Array.tabulate(second.length + 1)(identity)
      var current: Array[Int] = new Array[Int](second.length + 1)

      first.zipWithIndex.foreach:
        case (leftChar, leftIndex) =>
          current(0) = leftIndex + 1
          second.zipWithIndex.foreach:
            case (rightChar, rightIndex) =>
              val substitutionCost: Int = if leftChar == rightChar then 0 else 1
              current(rightIndex + 1) = (current(rightIndex) + 1)
                .min(previous(rightIndex + 1) + 1)
                .min(previous(rightIndex) + substitutionCost)
          val temporary: Array[Int] = previous
          previous = current
          current = temporary

      previous(second.length)

  private def assertSame(left: String, right: String): Unit =
    assertEquals(Levenshtein.distance(left, right), Reference.levenshtein(left, right),
      s"Levenshtein differs for [$left] x [$right]")
    assertEquals(DiceCoefficient.score(left, right), Reference.dice(left, right),
      s"Dice differs for [$left] x [$right]")

  private def randomString(random: Random, alphabet: IndexedSeq[Char], maxLength: Int): String =
    val length: Int = random.nextInt(maxLength + 1)
    String.valueOf(Array.fill(length)(alphabet(random.nextInt(alphabet.length))))

  test("Levenshtein known distances"):
    assertEquals(Levenshtein.distance("", ""), 0)
    assertEquals(Levenshtein.distance("abc", ""), 3)
    assertEquals(Levenshtein.distance("", "abc"), 3)
    assertEquals(Levenshtein.distance("kitten", "sitting"), 3)
    assertEquals(Levenshtein.distance("sitting", "kitten"), 3)
    assertEquals(Levenshtein.distance("flaw", "lawn"), 2)
    assertEquals(Levenshtein.distance("São Paulo", "Sao Paulo"), 1)
    assertEquals(Levenshtein.distance("abc", "abc"), 0)

  test("Dice known scores"):
    assertEquals(DiceCoefficient.score("night", "nacht"), 0.25)
    assertEquals(DiceCoefficient.score("abcd", "abcd"), 1.0)
    assertEquals(DiceCoefficient.score("abcd", "wxyz"), 0.0)
    // Repeated bigrams count once: {aa} x {aa}.
    assertEquals(DiceCoefficient.score("aaaa", "aa"), 1.0)
    // Strings shorter than two characters are compared by equality.
    assertEquals(DiceCoefficient.score("", ""), 1.0)
    assertEquals(DiceCoefficient.score("a", "a"), 1.0)
    assertEquals(DiceCoefficient.score("a", "b"), 0.0)
    assertEquals(DiceCoefficient.score("a", "ab"), 0.0)

  test("edge cases match the reference implementation"):
    val samples: Seq[String] = Seq(
      "", " ", "a", "aa", "ab", "ba", "aaaa", "abab", "abba",
      "ação", "acao", "São Paulo", "SAO PAULO",
      "耀老", "￿￿", "\u0000\u0000", "\u0000￿", "￿\u0000",
      "😀 emoji", "emoji 😀"
    )
    for left <- samples; right <- samples do assertSame(left, right)

  test("random strings over small alphabets match the reference implementation"):
    val random: Random = new Random(20261016L)
    val alphabets: Seq[IndexedSeq[Char]] = Seq(
      "ab".toIndexedSeq,
      "abc ".toIndexedSeq,
      "aeiouçãéíóúâêô ".toIndexedSeq
    )
    for
      alphabet <- alphabets
      _ <- 1 to 5000
    do assertSame(randomString(random, alphabet, 30), randomString(random, alphabet, 30))

  test("random strings over the whole Char range match the reference implementation"):
    val random: Random = new Random(42L)
    val allChars: IndexedSeq[Char] = (Char.MinValue to Char.MaxValue).toIndexedSeq
    for _ <- 1 to 5000 do
      assertSame(randomString(random, allChars, 40), randomString(random, allChars, 40))

  test("real LILACS titles match the reference implementation"):
    val path = Paths.get("csv/LILACS_MNT.csv")
    assume(Files.isReadable(path), s"$path not available")

    val titles: Vector[String] =
      Using.resource(new FileReader(path.toFile, StandardCharsets.UTF_8)): reader =>
        CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).get()
          .parse(reader).iterator().asScala
          .map(_.get("title_monographic"))
          .filter(title => title != null && title.nonEmpty)
          .take(3000)
          .toVector
    assert(titles.size > 1000, s"only ${titles.size} titles read")

    val random: Random = new Random(7L)
    // Neighbouring records plus random pairs, raw and normalized as the comparators do.
    val pairs: Iterator[(String, String)] =
      titles.sliding(2).map(pair => (pair(0), pair(1))) ++
        Iterator.fill(3000)((titles(random.nextInt(titles.size)), titles(random.nextInt(titles.size))))
    pairs.foreach: (left, right) =>
      assertSame(left, right)
      assertSame(Tools.normalizeStr(left), Tools.normalizeStr(right))
