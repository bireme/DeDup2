package dd

import org.apache.lucene.analysis.ngram.NGramTokenizer
import org.apache.lucene.analysis.{Analyzer, Tokenizer}

class NGAnalyzer(ngramSize: Int = 3,
                 search: Boolean = false) extends Analyzer {
  require (this.ngramSize >= 1)

  def getNgramSize: Int = ngramSize

  @Override
  def createComponents(fieldName: String): Analyzer.TokenStreamComponents = {
    val source: Tokenizer = if (search) new NGTokenizer(ngramSize) // generate side by size ngrams // current version
                            else new NGramTokenizer(ngramSize, ngramSize) // generate all ngrams

    new Analyzer.TokenStreamComponents(source)
  }
}
