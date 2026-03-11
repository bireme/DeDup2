package dd.comparators

import dd.interfaces.{CompResult, Comparator, Document}
import dd.tools.StringSimilarity.DiceCoefficient
import org.bireme.covid.SplitAuthors

import java.text.Normalizer
import java.text.Normalizer.Form
import scala.io.Source
import scala.util.{Failure, Success, Try, Using}

/**
 * Comparator specialized in matching author lists with normalization.
 *
 * The implementation expands author occurrences, normalizes naming variants,
 * and then compares the author sequences using similarity heuristics tailored
 * to abbreviated names and reordered author strings.
 */
class AuthorsComparator(fieldName: String) extends Comparator:
  private val occSeparator = ";"
  private val shortNameSimilarityThreshold = 0.4
  private val generalSimilarityThreshold = 0.5

  /**
   * Compares the input documents and returns the comparison result.
   *
   * @param originalDoc source document used in the comparison
   * @param currentDoc candidate document being evaluated
   * @return comparison result describing the evaluated documents
   */
  override def compare(originalDoc: Document,
                       currentDoc: Document): CompResult =
    val (rawOriSeq, oriSeq) = getAuthors(fieldName, originalDoc)
    val (rawCurSeq, curSeq) = getAuthors(fieldName, currentDoc)
    val originalAuthors = rawOriSeq.mkString(occSeparator)
    val currentAuthors = rawCurSeq.mkString(occSeparator)

    /**
     * Builds a comparison result for the current author sets.
     *
     * @param score score assigned to the generated comparison result
     * @param isSimilar flag indicating whether the compared values matched
     * @return comparison result created for the current author sets
     */
    def buildResult(score: Int, isSimilar: Boolean): CompResult =
      CompResult("AuthorsComparator", fieldName, originalAuthors, currentAuthors, None, None, score, isSimilar)

    if rawOriSeq.isEmpty && rawCurSeq.isEmpty then
      buildResult(score = 1, isSimilar = true)
    else if rawOriSeq.isEmpty != rawCurSeq.isEmpty then
      buildResult(score = 0, isSimilar = false)
    else
      val (fromSeq, toSeq) = if oriSeq.length <= curSeq.length then (oriSeq, curSeq) else (curSeq, oriSeq)

      if fromSeq.length < toSeq.length * 0.8 then
        buildResult(score = 0, isSimilar = false)
      else
        val isSimilar = fromSeq.forall(existSimilar(_, toSeq))
        buildResult(score = if isSimilar then 1 else 0, isSimilar = isSimilar)

  /**
   * Extracts and normalizes the authors stored in the selected field.
   *
   * @param fieldName field name associated with the operation
   * @param document document that provides the input values
   * @param fixOccSeparator whether occurrence separators should be normalized before splitting
   * @return raw and normalized author sequences
   */
  private def getAuthors(fieldName: String,
                         document: Document,
                         fixOccSeparator: Boolean = true): (Seq[String], Seq[String]) =
    val originalAuthors = document.fields.collect:
      case (`fieldName`, value) => value

    val rawAuthors =
      if fixOccSeparator then originalAuthors.flatMap(author => SplitAuthors.getAuthors(author, ",", occSeparator))
      else originalAuthors.flatMap(_.split(s" *$occSeparator *"))

    val normalizedAuthors = rawAuthors.map(normalizeAuthorName)

    (rawAuthors, normalizedAuthors)

  /**
   * Normalizes an author name for comparison.
   *
   * @param author author value to inspect
   * @return normalized author value
   */
  private def normalizeAuthorName(author: String): String =
    val trimmedName = author.replaceAll(" {2,}", " ").trim
    val invertedName = trimmedName.split(" *, *", 2)
    val orderedName = (if invertedName.length == 2 then invertedName.reverse else invertedName).mkString(" ")

    Normalizer.normalize(orderedName.toLowerCase(), Form.NFD)
      .replaceAll("[\\p{InCombiningDiacriticalMarks}]", "")
      .replace(".", "")

  /**
   * Checks whether an equivalent author exists in the given sequence.
   *
   * @param author author value to inspect
   * @param others candidate values checked against the input author
   * @return true when an equivalent author is found, false otherwise
   */
  private def existSimilar(author: String,
                           others: Seq[String]): Boolean =
    others.exists(isSimilar(_, author))

  /**
   * Checks whether the provided values should be considered similar.
   *
   * @param author1 first author value to compare
   * @param author2 second author value to compare
   * @return true when the provided values are considered similar, false otherwise
   */
  private def isSimilar(author1: String,
                        author2: String): Boolean =
    val similarity = getSimilarity(author1, author2)
    val hasCompatibleInitial = author1.nonEmpty && author2.nonEmpty && author1.head == author2.head
    val bothAreShortNames = author1.split(" ").length == 2 && author2.split(" ").length == 2

    (author1.isEmpty && author2.isEmpty) ||
      (hasCompatibleInitial && bothAreShortNames && similarity >= shortNameSimilarityThreshold) ||
      similarity >= generalSimilarityThreshold

  /**
   * Returns the similarity score for the provided values.
   *
   * @param str1 first value to compare
   * @param str2 second value to compare
   * @return similarity score for the provided values
   */
  private def getSimilarity(str1: String,
                            str2: String): Double =
    if str1.isEmpty != str2.isEmpty then 0
    else DiceCoefficient.score(str1, str2)

/**
 * Command-line utility that evaluates author-list similarity line by line.
 *
 * Each input line is expected to contain two author lists separated by `|`,
 * allowing quick manual inspection of cases that are not considered similar by
 * the comparator implementation.
 */
object AuthorsComparator:
  /**
   * Prints the command usage information and exits.
   * @return no value; this method terminates the application
   */
  private val usageMessage: String =
    """Identify if two lists of authors are similar. The authors are separated by ';'
      |usage: AuthorsComparator <authorsFileName>
      |where the each line of the file has the following pattern: <author1>;<author2>;...|<authorx1>;<authorx2>;...""".stripMargin

  /**
   * Entry point used when the utility is executed from the command line.
   *
   * @param args command-line arguments received by the utility
   * @return no value; this method delegates to the main workflow
   */
  def main(args: Array[String]): Unit =
    run(args) match
      case Success(_) => ()
      case Failure(exception) => Console.err.println(exception.getMessage)

  /**
   * Executes the command-line workflow for the authors comparator tool.
   *
   * @param args command-line arguments received by the utility
   * @return result of processing the provided input file
   */
  private def run(args: Array[String]): Try[Unit] =
    args.headOption match
      case Some(fileName) =>
        Using(Source.fromFile(fileName)):
          _.getLines().map(_.trim).filter(line => line.nonEmpty && line.head != '#').foreach(processLine)
      case None =>
        Failure(IllegalArgumentException(usageMessage))

  private def processLine(line: String): Unit =
    line.split("\\|", 2) match
      case Array(left, right) =>
        val originalDoc = Document(Seq("authors" -> left))
        val currentDoc = Document(Seq("authors" -> right))
        val result = new AuthorsComparator("authors").compare(originalDoc, currentDoc)

        Option.when(!result.isSimilar)(Seq(result.originalField, result.currentField, "")).foreach(_.foreach(println))
      case _ =>
        println(s"Invalid line format: $line")
