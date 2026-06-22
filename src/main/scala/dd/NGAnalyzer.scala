package dd

import org.apache.lucene.analysis.ngram.NGramTokenizer
import org.apache.lucene.analysis.{Analyzer, TokenStream, Tokenizer}

/**
 * Lucene analyzer that builds normalized n-gram token streams.
 *
 * The analyzer uses Lucene's standard n-gram tokenizer and always applies the
 * shared normalization filter afterward.
 */
class NGAnalyzer(ngramSize: Int = 3) extends Analyzer:
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
    val tokenizer: Tokenizer = new NGramTokenizer(ngramSize, ngramSize)
    val tokenStream: TokenStream = new NormalizeCharFilter(tokenizer)

    new Analyzer.TokenStreamComponents(tokenizer, tokenStream)
