package dd

import dd.comparators.DiceComparator
import dd.configurators.ConfMain
import dd.configurators.ConfMain.{ConfiguredReporter, SimilarDocsConfig}
import dd.interfaces.{CompResult, Comparator, DocsFinder, DocsProducer, Document, Heuristics, Reporter}
import ox.{Fork, forkUser, par, supervised}

import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import scala.util.{Failure, Success, Try}

/**
 * Core similarity-processing pipeline for source documents.
 *
 * The public constructor receives only the JSON configuration file. The
 * configuration supplies the source producer, finder, comparators, reporters,
 * and reporter-specific output fields.
 */
class SimilarDocs private[dd] (config: SimilarDocsConfig):
  /** Creates a similarity pipeline from a JSON configuration file. */
  def this(configFile: File) =
    this(ConfMain.parseSimilarDocsConfig(configFile).get)

  /** Creates a similarity pipeline from its runtime collaborators. */
  private[dd] def this(finder: DocsFinder,
                       filters: Seq[Comparator],
                       reporters: Seq[Reporter],
                       auxQuery: Option[String],
                       maxDocs: Option[Int],
                       otherFields: Seq[String],
                       heuristic: Option[Heuristics] = None) =
    this(
      SimilarDocsConfig(
        producer = new DocsProducer {
          /** Returns no source documents for the finder-only constructor. */
          override def getDocuments: LazyList[Document] = LazyList.empty },
        finder = finder,
        comparators = filters,
        reporters = reporters.map(reporter => ConfiguredReporter(reporter, otherFields)),
        auxQuery = auxQuery,
        maxDocs = maxDocs,
        heuristic = heuristic
      )
    )

  private val finder: DocsFinder = config.finder
  private val filters: Seq[Comparator] =
    SimilarDocs.includeIndexedFieldDiceComparator(config.finder, config.comparators)
  private val reporters: Seq[ConfiguredReporter] = config.reporters
  private val auxQuery: Option[String] = config.auxQuery
  private val maxDocs: Option[Int] = config.maxDocs
  private val documentParallelism: Int = config.documentParallelism
  private val reporterLock: Object = new Object

  /**
   * Runs the complete similarity workflow for the configured source producer.
   *
   * @return result of processing all configured source documents
   */
  def run(): Try[Unit] =
    closeAfter(processDocuments(config.producer.getDocuments, heuristic = config.heuristic))

  /** Processes the source documents with the configured worker parallelism. */
  private[dd] def processDocuments(
      docs: LazyList[Document],
      recoverDocumentError: (Document, Throwable) => Try[Unit] = (_, exception) => Failure(exception),
      heuristic: Option[Heuristics] = None
  ): Try[Unit] =
    Try:
      supervised:
        val iterator: Iterator[Document] = docs.iterator
        val iteratorLock: Object = new Object
        val completed: AtomicInteger = AtomicInteger(0)

        /** Returns the next document from the shared input iterator. */
        def nextDocument(): Option[Document] =
          iteratorLock.synchronized:
            if iterator.hasNext then Some(iterator.next()) else None

        /** Processes documents until the shared iterator is exhausted. */
        def processWorker(): Unit =
          var next: Option[Document] = nextDocument()
          while next.nonEmpty do
            val document: Document = next.get
            processSimilars(document, heuristic).recoverWith:
              case exception => recoverDocumentError(document, exception)
            .get

            val pos: Int = completed.incrementAndGet()
            if pos % 1000 == 0 then println(s"+++$pos")
            next = nextDocument()

        val workers: Seq[Fork[Unit]] = (1 to documentParallelism).map(_ => forkUser(processWorker()))
        workers.foreach(_.join())

  /**
   * Processes the similar documents for the given source document.
   *
   * @param originalDoc source document used in the comparison
   * @param heuristic optional duplicate heuristic used to filter candidates
   * @return result of processing the matched documents
   */
  def processSimilars(originalDoc: Document,
                      heuristic: Option[Heuristics] = None): Try[Unit] =
    similar(originalDoc, heuristic).flatMap:
      _.foldLeft(Try(())):
        case (acc, (currentDoc, results)) =>
          acc.flatMap(_ => notifyReporters(originalDoc, currentDoc, results))

  /**
   * Closes the underlying resources.
   * @return result of closing the underlying resources
   */
  def close(): Try[Unit] =
    val closeResults: Seq[Try[Unit]] = finder.close() +: reporters.map(_.reporter.close())
    closeResults.collectFirst:
      case Failure(exception) => Failure(exception)
    .getOrElse(Success(()))

  /**
   * Finds candidate documents and computes comparison results for them.
   *
   * @param originalDoc source document used in the search and comparison
   * @param heuristic optional duplicate heuristic applied to compared
   *                  candidate documents; when absent, all candidates remain
   * @return matched documents paired with their comparison results
   */
  private def similar(originalDoc: Document,
                      heuristic: Option[Heuristics]): Try[LazyList[(Document, Seq[CompResult])]] =
    for
      searchField <- finder.getSearchField.toRight(IllegalArgumentException("Empty search field")).toTry
      query <- originalDoc.fields.collectFirst:
        case (`searchField`, value) => value
      .toRight(IllegalArgumentException("Empty search field")).toTry
      producer <- maxDocs match
        case Some(value) => finder.findDocs(searchField, query, auxQuery, value)
        case None => finder.findDocs(searchField, query, auxQuery)
    yield
      val comparedDocuments: LazyList[(Document, Seq[CompResult])] = getResults(originalDoc, producer.getDocuments)
      heuristic match
        case None => comparedDocuments
        case Some(value) => comparedDocuments.filter:
          case (document, results) => value.isDuplicated(document, results)

  /**
   * Builds the comparison results for the provided documents.
   *
   * @param originalDoc source document used in the comparison
   * @param docs documents to compare against the source document
   * @return comparison results built from the provided input
   */
  private def getResults(originalDoc: Document,
                         docs: LazyList[Document]): LazyList[(Document, Seq[CompResult])] =
    docs.map(doc => getResults(originalDoc, doc))

  /**
   * Builds the comparison results for the provided documents.
   *
   * @param originalDoc source document used in the comparison
   * @param currentDoc candidate document being evaluated
   * @return comparison results built from the provided input
   */
  private def getResults(originalDoc: Document,
                         currentDoc: Document): (Document, Seq[CompResult]) =
    val results: Seq[CompResult] = par(filters.map(comparator => () => comparator.compare(originalDoc, currentDoc)))
    (currentDoc, results)

  /**
   * Sends the comparison results to every configured reporter in sequence.
   *
   * @param originalDoc source document used in the comparison
   * @param currentDoc candidate document being evaluated
   * @param results comparison results produced for the document pair
   * @return result of notifying all configured reporters
   */
  private def notifyReporters(originalDoc: Document,
                              currentDoc: Document,
                              results: Seq[CompResult]): Try[Unit] =
    reporterLock.synchronized:
      reporters.foldLeft(Try(())):
        case (acc, configuredReporter) =>
          acc.flatMap:
            _ =>
              configuredReporter.reporter.writeResults(
                originalDoc,
                currentDoc,
                configuredReporter.otherFields,
                results
              )

  /**
   * Runs an operation and closes the similarity pipeline afterward.
   *
   * @param operation operation to execute before closing resources
   * @return result of the operation, preserving close failures when appropriate
   */
  private def closeAfter(operation: => Try[Unit]): Try[Unit] =
    val operationResult: Try[Unit] = Try(operation).flatten
    val closeResult: Try[Unit] = close()

    operationResult match
      case Success(_) => closeResult
      case Failure(exception) =>
        closeResult.failed.foreach(exception.addSuppressed)
        Failure(exception)

/**
 * Command-line entrypoint for the generic similarity-processing pipeline.
 *
 * This object accepts only a configuration file path. Runtime parameters that
 * used to be passed through the command line now belong to the configured
 * producer, finder, comparator, and reporter blocks.
 */
object SimilarDocs:
  private val requiredSchemaFields: Seq[String] = Seq("dbase", "id")

  /**
   * Prints the command usage information and exits.
   * @return no value; this method terminates the application
   */
  private val usageMessage: String =
    """\nCheck for duplicated documents in a database/index.
      |
      |usage: SimilarDocs <configFile>
      |
      |<configFile>:
      |     JSON configuration file containing producer, finder, comparators, and reporters.""".stripMargin

  /**
   * Entry point used when the application is executed from the command line.
   *
   * @param args command-line arguments received by the application
   * @return no value; this method delegates to the main workflow
   */
  def main(args: Array[String]): Unit =
    run(args).recover:
      case exception =>
        Console.err.println(exception.getMessage)
        sys.exit(1)

  /**
   * Executes the command-line workflow with the provided arguments.
   *
   * @param args command-line arguments received by the application
   * @return result of the complete similarity-processing run
   */
  private def run(args: Array[String]): Try[Unit] =
    for
      configFile <- parseConfigFile(args)
      similarDocs <- Try(new SimilarDocs(configFile))
      _ <- similarDocs.run()
    yield ()

  /**
   * Parses the command-line configuration file argument.
   *
   * @param args command-line arguments
   * @return parsed configuration file
   */
  private def parseConfigFile(args: Array[String]): Try[File] =
    args.toSeq match
      case Seq(value) if value.startsWith("-confFile=") && value.length > "-confFile=".length =>
        Success(new File(value.substring("-confFile=".length)))
      case Seq(value) if value.nonEmpty && !value.startsWith("-") =>
        Success(new File(value))
      case _ =>
        Failure(IllegalArgumentException(usageMessage))

  /**
   * Validates whether the input schema contains all required report identifiers.
   *
   * @param schema input CSV schema mapping column positions to field names
   * @return successful result when all required fields are present
   */
  private[dd] def requireSchemaFields(schema: Map[Int, String]): Try[Unit] =
    val schemaFields: Set[String] = schema.values.toSet
    val missing: Seq[String] = requiredSchemaFields.filterNot(schemaFields.contains)
    if missing.isEmpty then Success(())
    else
      Failure(IllegalArgumentException(s"Schema missing required field(s): ${missing.mkString(", ")}"))

  /**
   * Prepends required report fields while preserving caller-provided fields.
   *
   * @param otherFields additional fields requested by the user
   * @return report fields including required identifiers without duplicates
   */
  private[dd] def includeRequiredReportFields(otherFields: Seq[String]): Seq[String] =
    ConfMain.includeRequiredReportFields(otherFields)

  /**
   * Ensures the Lucene indexed field is also present in reportable comparison
   * results, without calculating the same Dice comparison twice.
   *
   * @param finder configured finder that supplies the indexed field and threshold
   * @param comparators comparator list parsed from configuration
   * @return comparator list with one Dice comparator for the indexed field
   */
  private[dd] def includeIndexedFieldDiceComparator(finder: DocsFinder,
                                                   comparators: Seq[Comparator]): Seq[Comparator] =
    (finder.getSearchField, finder.getMinSimilarity) match
      case (Some(searchField), Some(minSimilarity)) if !hasDiceComparatorFor(comparators, searchField) =>
        DiceComparator(searchField, normalize = true, minSimilarity) +: comparators
      case _ => comparators

  /** Indicates whether a Dice comparator covers the indexed field. */
  private def hasDiceComparatorFor(comparators: Seq[Comparator],
                                   fieldName: String): Boolean =
    comparators.exists:
      case dice: DiceComparator => dice.fieldName == fieldName
      case _ => false
