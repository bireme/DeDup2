package dd

import org.apache.lucene.analysis.Tokenizer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute

import scala.annotation.tailrec

class NGTokenizer(ngramSize: Int) extends Tokenizer {
  require(this.ngramSize >= 1)

  private  val termAtt: CharTermAttribute = addAttribute(classOf[CharTermAttribute])
  termAtt.resizeBuffer(ngramSize)

  private var doubleTok: Boolean = input.markSupported()

  def getNgramSize: Int = ngramSize

  @Override
  def incrementToken(): Boolean = {
    clearAttributes()
    getNextToken
  }

  private def getNextToken: Boolean = {
    termAtt.setEmpty()
    getNextToken(pos = 0)
  }

  @tailrec
  private def getNextToken(pos: Int): Boolean = {
    assert (pos >= 0)

    if (pos == ngramSize) true
    else {
      val ich: Int = input.read()
      if (ich == -1) {
        if (doubleTok) {
          doubleTok = false
          input.reset()
          input.skip(ngramSize - 1)  // Replace the initial position of tokenization
          getNextToken
        } else {
          termAtt.setEmpty()
          false
        }
      } else {
        val ch: Char = ich.toChar
        if (ch == ' ') getNextToken
        else {
          termAtt.append(ch)
          getNextToken(pos + 1)
        }
      }
    }
  }
}
