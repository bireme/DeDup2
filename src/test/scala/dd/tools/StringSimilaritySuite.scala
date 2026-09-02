package dd.tools

import dd.tools.StringSimilarity.{DiceCoefficient, Jaccard, Levenshtein, LevenshteinJaccard}

class StringSimilaritySuite extends munit.FunSuite:
  test("word normalization preserves boundaries and collapses whitespace"):
    assertEquals(Tools.normalizeWordsStr("  São   Paulo, Brasil!  "), "sao paulo brasil")

  test("Levenshtein distance computes edit operations"):
    assertEquals(Levenshtein.distance("kitten", "sitting"), 3)

  test("Levenshtein score is normalized"):
    assertEquals(Levenshtein.score("same", "same"), 1.0)
    assertEquals(Levenshtein.score("", "abc"), 0.0)

  test("Jaccard compares distinct words"):
    assertEquals(Jaccard.score("one two three", "two three four"), 0.5)

  test("Jaccard treats two empty strings as equal"):
    assertEquals(Jaccard.score("", "   "), 1.0)

  test("LevenshteinJaccard combines the component scores with the specified weights"):
    val expected = 0.6 * Levenshtein.score("one two", "one three") +
      0.4 * Jaccard.score("one two", "one three")
    assertEquals(LevenshteinJaccard.score("one two", "one three"), expected)

  test("existing Dice coefficient remains available"):
    assertEquals(DiceCoefficient.score("abcd", "abcd"), 1.0)
