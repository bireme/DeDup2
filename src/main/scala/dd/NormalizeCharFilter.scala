package dd

import org.apache.lucene.analysis.{TokenFilter, TokenStream}
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute

import java.text.Normalizer

class NormalizeCharFilter(input: TokenStream) extends TokenFilter(input) {
  private val termAttr: CharTermAttribute = addAttribute(classOf[CharTermAttribute])

  override def incrementToken(): Boolean = {
    if (!input.incrementToken()) return false

    val original = termAttr.toString
    val normalized = normalizeText(original)

    termAttr.setEmpty().append(normalized)
    true
  }

  private def normalizeText(text: String): String = {
    val noAccents = Normalizer.normalize(text, Normalizer.Form.NFD)
      .replaceAll("\\p{M}", "") // Remove marcas diacríticas (acentos)

    noAccents
      .toLowerCase()
      .replace('ç', 'c') // Substitui cedilha
  }
}