package dd

import dd.configurators.ConfMain
import dd.configurators.ConfMain.{ConfiguredReporter, SimilarDocsConfig}
import dd.interfaces.{CompResult, Comparator, DocsFinder, DocsProducer, Document, Reporter}

import java.io.File
import scala.util.{Failure, Success, Try}

/**
 * Core similarity-processing pipeline for source documents.
 *
 * The public constructor receives only the JSON configuration file. The
 * configuration supplies the source producer, finder, comparators, reporters,
 * and reporter-specific output fields.
 */
class SimilarDocs private[dd] (config: SimilarDocsConfig):
  def this(configFile: File) =
    this(ConfMain.parseSimilarDocsConfig(configFile).get)

  private[dd] def this(finder: DocsFinder,
                       filters: Seq[Comparator],
                       reporters: Seq[Reporter],
                       auxQuery: Option[String],
                       maxDocs: Option[Int],
                       otherFields: Seq[String]) =
    this(
      SimilarDocsConfig(
        producer = new DocsProducer:
          override def getDocuments: LazyList[Document] = LazyList.empty,
        finder = finder,
        comparators = filters,
        reporters = reporters.map(reporter => ConfiguredReporter(reporter, otherFields)),
        auxQuery = auxQuery,
        maxDocs = maxDocs
      )
    )

  private val finder: DocsFinder = config.finder
  private val filters: Seq[Comparator] = config.comparators
  private val reporters: Seq[ConfiguredReporter] = config.reporters
  private val auxQuery: Option[String] = config.auxQuery
  private val maxDocs: Option[Int] = config.maxDocs

  /**
   * Runs the complete similarity workflow for the configured source producer.
   *
   * @return result of processing all configured source documents
   */
  def run(): Try[Unit] =
    closeAfter:
      config.producer.getDocuments.zipWithIndex.foldLeft(Try(())):
        case (acc, (document, index)) =>
          acc.flatMap:
            _ =>
              processSimilars(document).map:
                _ =>
                  val pos = index + 1
                  if pos % 100 == 0 then println(s"+++$pos")

  /**
   * Processes the similar documents for the given source document.
   *
   * @param originalDoc source document used in the comparison
   * @return result of processing the matched documents
   */
  def processSimilars(originalDoc: Document): Try[Unit] =
    similar(originalDoc).flatMap:
      _.foldLeft(Try(())):
        case (acc, (currentDoc, results)) =>
          acc.flatMap(_ => notifyReporters(originalDoc, currentDoc, results))

  /**
   * Closes the underlying resources.
   * @return result of closing the underlying resources
   */
  def close(): Try[Unit] =
    val closeResults = finder.close() +: reporters.map(_.reporter.close())
    closeResults.collectFirst:
      case Failure(exception) => Failure(exception)
    .getOrElse(Success(()))

  /**
   * Finds candidate documents and computes comparison results for them.
   *
   * @param originalDoc source document used in the search and comparison
   * @return matched documents paired with their comparison results
   */
  private def similar(originalDoc: Document): Try[LazyList[(Document, Seq[CompResult])]] =
    for
      searchField <- finder.getSearchField.toRight(IllegalArgumentException("Empty search field")).toTry
      query <- originalDoc.fields.collectFirst:
        case (`searchField`, value) => value
      .toRight(IllegalArgumentException("Empty search field")).toTry
      producer <- maxDocs match
        case Some(value) => finder.findDocs(searchField, query, auxQuery, value)
        case None => finder.findDocs(searchField, query, auxQuery)
    yield getResults(originalDoc, producer.getDocuments)

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
    (currentDoc, filters.map(_.compare(originalDoc, currentDoc)))

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
    if results.zip(filters).exists:
      case (result, comparator) => comparator.isGate && result.isSimilar
    then
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
    else Success(())

  /**
   * Runs an operation and closes the similarity pipeline afterward.
   *
   * @param operation operation to execute before closing resources
   * @return result of the operation, preserving close failures when appropriate
   */
  private def closeAfter(operation: => Try[Unit]): Try[Unit] =
    val operationResult = Try(operation).flatten
    val closeResult = close()

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
