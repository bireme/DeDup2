package dd.comparators

import dd.interfaces.{CompResult, Comparator, Document, PreparedComparator, SimilarityStatus}
import dd.tools.StringSimilarity.DiceCoefficient
import dd.tools.StringSimilarity.DiceCoefficient.Bigrams
import org.apache.commons.csv.CSVFormat

import java.io.StringReader
import java.text.Normalizer
import java.text.Normalizer.Form
import java.util.regex.Pattern
import scala.jdk.CollectionConverters.*
import scala.io.Source
import scala.util.{Failure, Success, Try, Using}

/**
 * Comparator specialized in matching author lists with normalization.
 *
 * The implementation expands author occurrences, normalizes naming variants,
 * and then compares the author sequences using similarity heuristics tailored
 * to abbreviated names and reordered author strings.
 */
class AuthorsComparator(fieldName: String,
                        occSeparator: String = "//@//") extends Comparator:
  private val shortNameSimilarityThreshold: Double = 0.4
  private val generalSimilarityThreshold: Double = 0.5

  // Patterns and CSV format built once: authors are extracted for every compared document.
  private val occSeparatorPattern: Pattern = Pattern.compile(Pattern.quote(occSeparator))
  private lazy val spacedOccSeparatorPattern: Pattern = Pattern.compile(s" *$occSeparator *")
  private val semicolonFormat: CSVFormat = CSVFormat.Builder.create().setDelimiter(';').setTrim(true).get()

  /**
   * Normalized author name with the values reused by every comparison.
   *
   * @param name normalized author name
   * @param bigrams precomputed Dice bigrams of {@code name}
   * @param isShortName whether the name has exactly two space-separated words
   */
  private case class Author(name: String, bigrams: Bigrams, isShortName: Boolean)

  /**
   * Authors of one document.
   *
   * @param joined raw author names joined by the occurrence separator
   * @param authors normalized authors
   */
  private case class Authors(joined: String, authors: Seq[Author])

  /**
   * Prepares the comparator for a source document, extracting its authors once.
   *
   * @param originalDoc source document used in every subsequent comparison
   * @return comparator bound to the source document
   */
  override def prepare(originalDoc: Document): PreparedComparator =
    val original: Authors = documentAuthors(originalDoc)
    currentDoc => compareAuthors(original, documentAuthors(currentDoc))

  /**
   * Compares the input documents and returns the comparison result.
   *
   * @param originalDoc source document used in the comparison
   * @param currentDoc candidate document being evaluated
   * @return comparison result describing the evaluated documents
   */
  override def compare(originalDoc: Document,
                       currentDoc: Document): CompResult =
    prepare(originalDoc).compare(currentDoc)

  /**
   * Compares the authors of two documents.
   *
   * @param original authors of the source document
   * @param current authors of the candidate document
   * @return comparison result describing the evaluated documents
   */
  private def compareAuthors(original: Authors,
                             current: Authors): CompResult =
    /**
     * Builds a comparison result for the current author sets.
     *
     * @param score score assigned to the generated comparison result
     * @param status final similarity status for the generated comparison result
     * @return comparison result created for the current author sets
     */
    def buildResult(score: Int,
                    status: SimilarityStatus): CompResult =
      CompResult("AuthorsComparator", fieldName, original.joined, current.joined, None, None, score, status)

    if original.joined.trim.isEmpty || current.joined.trim.isEmpty then
      buildResult(score = 0, status = SimilarityStatus.undefined)
    else
      val oriSeq: Seq[Author] = original.authors
      val curSeq: Seq[Author] = current.authors
      val (fromSeq, toSeq) = if oriSeq.length <= curSeq.length then (oriSeq, curSeq) else (curSeq, oriSeq)

      if fromSeq.length < toSeq.length * 0.8 then
        buildResult(score = 0, status = SimilarityStatus.no)
      else
        val isSimilar: Boolean = fromSeq.forall(existSimilar(_, toSeq))
        val status = if isSimilar then SimilarityStatus.yes else SimilarityStatus.no
        buildResult(score = if isSimilar then 1 else 0, status = status)

  /**
   * Extracts the raw and normalized authors of a document.
   *
   * @param document document that provides the input values
   * @return authors of the document
   */
  private def documentAuthors(document: Document): Authors =
    val (rawAuthors, normalizedAuthors) = getAuthors(fieldName, document)
    Authors(rawAuthors.mkString(occSeparator), normalizedAuthors.map(toAuthor))

  /**
   * Precomputes the comparison values of a normalized author name.
   *
   * @param name normalized author name
   * @return author with its precomputed values
   */
  private def toAuthor(name: String): Author =
    Author(name, DiceCoefficient.bigrams(name), name.split(" ").length == 2)

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
    val originalAuthors: Seq[String] = document.fields.collect:
      case (`fieldName`, value) => value

    val rawAuthors: Seq[String] =
      if fixOccSeparator then originalAuthors.flatMap(author => getAuthors(author, occSeparator))
      else originalAuthors.flatMap(spacedOccSeparatorPattern.split(_).toSeq)

    val normalizedAuthors: Seq[String] = rawAuthors.map(normalizeAuthorName)

    (rawAuthors, normalizedAuthors)

  /** Extracts normalized author names from a document field. */
  private def getAuthors(field: String,
                         authorSeparator: String): Seq[String] =
    if field.isEmpty then Seq[String]()
    else if field.contains(authorSeparator) then occSeparatorPattern.split(field).toSeq // line already processed
    else if field.contains(";") then separateBySemicolon(field)
    else Seq(field.trim)

  /**
   * Splits an author field into non-empty semicolon-separated values.
   *
   * @param field author field value
   * @return non-empty author values
   */
  private def separateBySemicolon(field: String): Seq[String] =
    semicolonFormat.parse(new StringReader(field)).asScala.head.asScala.toSeq.map(_.trim)

  /**
   * Normalizes an author name for comparison.
   *
   * @param author author value to inspect
   * @return normalized author value
   */
  private def normalizeAuthorName(author: String): String =
    val trimmedName: String = AuthorsComparator.RepeatedSpaces.matcher(author).replaceAll(" ").trim
    val invertedName: Array[String] = AuthorsComparator.Comma.split(trimmedName, 2)
    val orderedName: String = (if invertedName.length == 2 then invertedName.reverse else invertedName).mkString(" ")

    AuthorsComparator.Diacritics.matcher(Normalizer.normalize(orderedName.toLowerCase(), Form.NFD))
      .replaceAll("")
      .replace(".", "")

  /**
   * Checks whether an equivalent author exists in the given sequence.
   *
   * @param author author value to inspect
   * @param others candidate values checked against the input author
   * @return true when an equivalent author is found, false otherwise
   */
  private def existSimilar(author: Author,
                           others: Seq[Author]): Boolean =
    others.exists(isSimilar(_, author))

  /**
   * Checks whether the provided values should be considered similar.
   *
   * @param author1 first author value to compare
   * @param author2 second author value to compare
   * @return true when the provided values are considered similar, false otherwise
   */
  private def isSimilar(author1: Author,
                        author2: Author): Boolean =
    val similarity: Double = getSimilarity(author1, author2)
    val name1: String = author1.name
    val name2: String = author2.name
    val hasCompatibleInitial: Boolean = name1.nonEmpty && name2.nonEmpty && name1.head == name2.head
    val bothAreShortNames: Boolean = author1.isShortName && author2.isShortName

    (name1.isEmpty && name2.isEmpty) ||
      (hasCompatibleInitial && bothAreShortNames && similarity >= shortNameSimilarityThreshold) ||
      similarity >= generalSimilarityThreshold

  /**
   * Returns the similarity score for the provided values.
   *
   * @param author1 first value to compare
   * @param author2 second value to compare
   * @return similarity score for the provided values
   */
  private def getSimilarity(author1: Author,
                            author2: Author): Double =
    if author1.name.isEmpty != author2.name.isEmpty then 0
    else DiceCoefficient.score(author1.bigrams, author2.bigrams)

/**
 * Command-line utility that evaluates author-list similarity line by line.
 *
 * Each input line is expected to contain two author lists separated by `|`,
 * allowing quick manual inspection of cases that are not considered similar by
 * the comparator implementation.
 */
object AuthorsComparator:
  private val RepeatedSpaces: Pattern = Pattern.compile(" {2,}")
  private val Comma: Pattern = Pattern.compile(" *, *")
  private val Diacritics: Pattern = Pattern.compile("[\\p{InCombiningDiacriticalMarks}]")

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

  /**
   * Processes one command-line input pair and prints non-similar author lists.
   *
   * @param line input line containing two author lists separated by a pipe
   * @return no value; this method writes diagnostic output when needed
   */
  private def processLine(line: String): Unit =
    line.split("\\|", 2) match
      case Array(left, right) =>
        val originalDoc: Document = Document(Seq("authors" -> left))
        val currentDoc: Document = Document(Seq("authors" -> right))
        val result: CompResult = new AuthorsComparator("authors").compare(originalDoc, currentDoc)

        Option.when(result.isSimilar != SimilarityStatus.yes)(Seq(result.originalField, result.currentField, "")).foreach(_.foreach(println))
      case _ =>
        println(s"Invalid line format: $line")
