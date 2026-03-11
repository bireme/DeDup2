package dd

import org.apache.lucene.analysis.ngram.NGramTokenizer
import org.apache.lucene.analysis.{Analyzer, TokenStream, Tokenizer}

/**
 * Lucene analyzer that builds normalized n-gram token streams.
 *
 * Depending on the search mode, the analyzer either uses the custom tokenizer
 * optimized for querying or Lucene's standard n-gram tokenizer for indexing,
 * always applying the shared normalization filter afterward.
 */
class NGAnalyzer(ngramSize: Int = 3,
                 search: Boolean = false) extends Analyzer:
  require (this.ngramSize >= 1)

  /**
   * Returns the configured n-gram size.
   * @return configured n-gram size
   */
  def getNgramSize: Int = ngramSize

  @Override
  /**
   * Creates the token stream components for the given field.
   *
   * @param fieldName field name associated with the operation
   * @return token stream components for the given field
   */
  def createComponents(fieldName: String): Analyzer.TokenStreamComponents =
    val tokenizer: Tokenizer = if search then new NGTokenizer(ngramSize) // generate side by size ngrams // current version
                            else new NGramTokenizer(ngramSize, ngramSize) // generate all ngrams
    val tokenStream: TokenStream = new NormalizeCharFilter(tokenizer)

    new Analyzer.TokenStreamComponents(tokenizer, tokenStream)
