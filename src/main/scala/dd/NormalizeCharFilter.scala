package dd

import org.apache.lucene.analysis.{TokenFilter, TokenStream}
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute

import java.text.Normalizer

/**
 * Token filter that normalizes token text before indexing or searching.
 *
 * The filter removes accent marks, lowercases the token content, and applies
 * a small set of character-level replacements so textual comparisons become
 * more stable across input variants.
 */
class NormalizeCharFilter(input: TokenStream) extends TokenFilter(input):
  private val termAttr: CharTermAttribute = addAttribute(classOf[CharTermAttribute])

  /**
   * Advances to the next token in the stream.
   * @return true when a token was produced, false otherwise
   */
  override def incrementToken(): Boolean =
    if input.incrementToken() then
      val original = termAttr.toString
      val normalized = normalizeText(original)

      termAttr.setEmpty().append(normalized)
      true
    else false

  /**
   * Normalizes the token text for indexing.
   *
   * @param text token text to normalize
   * @return normalized token text
   */
  private def normalizeText(text: String): String =
    val noAccents = Normalizer.normalize(text, Normalizer.Form.NFD)
      .replaceAll("\\p{M}", "") // Remove marcas diacríticas (acentos)

    noAccents
      .toLowerCase()
      .replace('ç', 'c') // Substitui cedilha
