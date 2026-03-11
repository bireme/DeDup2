package dd

import org.apache.lucene.analysis.Tokenizer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute

import scala.annotation.tailrec

/**
 * Tokenizer that emits fixed-size n-grams for search-time matching.
 *
 * The tokenizer reads characters from the Lucene input stream, skips blanks,
 * and performs a second replay pass when supported so overlapping grams can be
 * generated from the same source content.
 */
class NGTokenizer(ngramSize: Int) extends Tokenizer:
  require(this.ngramSize >= 1)

  private  val termAtt: CharTermAttribute = addAttribute(classOf[CharTermAttribute])
  termAtt.resizeBuffer(ngramSize)

  private enum TokenizationState:
    case FirstPass, ReplayPass, Finished

  private var tokenizationState: TokenizationState = TokenizationState.FirstPass

  /**
   * Returns the configured n-gram size.
   * @return configured n-gram size
   */
  def getNgramSize: Int = ngramSize

  @Override
  /**
   * Advances to the next token in the stream.
   * @return true when a token was produced, false otherwise
   */
  def incrementToken(): Boolean =
    clearAttributes()
    getNextToken

  /**
   * Reads the next available token.
   * @return true when a token was produced, false otherwise
   */
  private def getNextToken: Boolean =
    termAtt.setEmpty()
    getNextToken(pos = 0)

  @tailrec
  /**
   * Reads the next available token.
   *
   * @param pos value of pos
   * @return true when a token was produced, false otherwise
   */
  private def getNextToken(pos: Int): Boolean =
    assert (pos >= 0)

    if pos == ngramSize then true
    else
      val ich: Int = input.read()
      if ich == -1 then
        moveToNextPass() match
          case Some(_) => getNextToken
          case None =>
            termAtt.setEmpty()
            false
      else
        val ch: Char = ich.toChar
        if ch == ' ' then getNextToken
        else
          termAtt.append(ch)
          getNextToken(pos + 1)

  /**
   * Moves the tokenizer to the replay pass when the input supports it.
   *
   * @return next tokenization state when a replay pass is available
   */
  private def moveToNextPass(): Option[TokenizationState] =
    tokenizationState match
      case TokenizationState.FirstPass if input.markSupported() =>
        tokenizationState = TokenizationState.ReplayPass
        input.reset()
        input.skip(ngramSize - 1)  // Replace the initial position of tokenization
        Some(tokenizationState)
      case TokenizationState.FirstPass | TokenizationState.ReplayPass | TokenizationState.Finished =>
        tokenizationState = TokenizationState.Finished
        None
