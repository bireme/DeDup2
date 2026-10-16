package dd.tools

import java.text.Normalizer
import java.text.Normalizer.Form
import scala.util.Random

/**
 * Checks that the normalizers with precompiled patterns return exactly the same
 * values as the original {@code String.replaceAll} implementations.
 */
class NormalizationEquivalenceSuite extends munit.FunSuite:
  private def referenceNormalizeStr(in: String): String =
    Normalizer.normalize(in.trim().toLowerCase, Form.NFD).replaceAll("[^a-z0-9]", "")

  private def referenceNormalizeWordsStr(in: String): String =
    Normalizer.normalize(in.toLowerCase, Form.NFD)
      .replaceAll("\\p{M}", "")
      .replaceAll("[^a-z0-9\\s]", " ")
      .replaceAll("\\s+", " ")
      .trim

  private def assertSame(value: String): Unit =
    assertEquals(Tools.normalizeStr(value), referenceNormalizeStr(value), s"normalizeStr differs for [$value]")
    assertEquals(Tools.normalizeWordsStr(value), referenceNormalizeWordsStr(value),
      s"normalizeWordsStr differs for [$value]")

  test("edge cases match the reference implementation"):
    Seq(
      "", " ", "\t\n\r\f\u000B", " ", "  São   Paulo, Brasil!  ", "Ação-Saúde/2024",
      "é", "ÀÉÎÕÜ ñ ç", "ﬁ ligature", "İstanbul", "ß", "Ω≈ç√∫", "😀 emoji",
      "a\tb\nc", "...", "123-456", "e123"
    ).foreach(assertSame)

  test("random strings match the reference implementation"):
    val random: Random = new Random(20261016L)
    val alphabet: IndexedSeq[Char] = "aAzZ09 \t\n.,;-/()áÁãçÇéêíóõúüñ́̃ ßİ".toIndexedSeq
    for _ <- 1 to 20000 do
      assertSame(String.valueOf(Array.fill(random.nextInt(25))(alphabet(random.nextInt(alphabet.length)))))
