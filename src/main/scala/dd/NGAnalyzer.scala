package dd

import org.apache.lucene.analysis.ngram.NGramTokenizer
import org.apache.lucene.analysis.{Analyzer, TokenStream, Tokenizer}

class NGAnalyzer(ngramSize: Int = 3,
                 search: Boolean = false) extends Analyzer {
  require (this.ngramSize >= 1)

  def getNgramSize: Int = ngramSize

  @Override
  def createComponents(fieldName: String): Analyzer.TokenStreamComponents = {
    val tokenizer: Tokenizer = if (search) new NGTokenizer(ngramSize) // generate side by size ngrams // current version
                            else new NGramTokenizer(ngramSize, ngramSize) // generate all ngrams
    val tokenStream: TokenStream = new NormalizeCharFilter(tokenizer)

    new Analyzer.TokenStreamComponents(tokenizer, tokenStream)
  }
}
