package dd

import dd.configurators.ConfMain
import dd.configurators.ConfMain.{ConfiguredReporter, SelfCheckCsvSourceConfig, SelfCheckDuplicatedConfig, SelfCheckMysqlSourceConfig, SimilarDocsConfig}
import dd.finders.LuceneDocsFinder
import dd.interfaces.{CompResult, DocsProducer, Document, Reporter}
import dd.producers.CSVProducer
import dd.tools.{CSV2Lucene, SQL2CSV}

import java.io.File
import java.nio.file.Files
import scala.collection.mutable
import scala.io.{Codec, Source}
import scala.util.{Failure, Success, Try, Using}

/**
 * End-to-end self-check workflow for duplicate documents.
 *
 * The command receives the same JSON configuration file accepted by
 * SimilarDocs, prepares the configured producer as CSV, builds a temporary
 * Lucene index from that CSV, and processes the same CSV against the generated
 * index while skipping self-pairs.
 */
object SelfCheckDuplicated:
  private val usageMessage: String =
    """Check duplicated documents from a SQL result set using a temporary CSV/Lucene flow.
      |
      |usage: SelfCheckDuplicated <configFile>
      |
      |<configFile>:
      |     JSON configuration file containing producer/mysql or producer/csv, finder/lucene, comparators, and reporters.
      |
      |Optional JSON block:
      |     "selfCheckDuplicated": {
      |       "outCsvFile": "path/to/generated.csv",
      |       "index": "path/to/generated-lucene-index",
      |       "encoding": "utf-8"
      |     }""".stripMargin

  /**
   * Entry point used when the self-check workflow is executed from the command line.
   *
   * @param args command-line arguments received by the application
   * @return no value; this method exits the process when the workflow fails
   */
  def main(args: Array[String]): Unit =
    run(args) match
      case Success(_) => println("Self duplicate check finished successfully!")
      case Failure(exception) =>
        Console.err.println(exception.getMessage)
        sys.exit(1)

  /**
   * Executes the complete self-duplicate detection workflow.
   *
   * The workflow loads the shared JSON configuration, prepares a CSV source,
   * creates a temporary Lucene index, and processes each source document against
   * the generated index while skipping comparisons between the same document.
   *
   * @param args command-line arguments containing the configuration file path
   * @return successful result when the complete workflow finishes
   */
  def run(args: Array[String]): Try[Unit] =
    for
      configFile <- parseConfigFile(args)
      config <- ConfMain.parseSelfCheckDuplicatedConfig(configFile)
      index <- outputIndex(config)
      source <- prepareSource(config)
      _ = print(s"Generating Lucene index: $index ... ")
      _ <- CSV2Lucene.run(csv2LuceneArgs(config, source, index))
      _ = println("OK")
      csvProducer = new CSVProducer(
        source.csvFile,
        source.schema,
        source.hasHeader,
        source.fieldSeparator,
        source.encoding
      )
      _ = print("Generating similar documents ... ")
      similarDocs = createSimilarDocs(config, index)
      _ <- closeAfter(similarDocs):
        similarDocs.processDocuments(
          csvProducer.getDocuments,
          (document, ex) =>
            Console.err.println(s"Processing similars error. msg=${ex.toString} doc=${document.toString}")
            ex.printStackTrace()
            Success(())
        )
      _ = println("OK")
    yield ()

  /**
   * Parses the configuration file argument accepted by the command line entry point.
   *
   * @param args command-line arguments received by the application
   * @return configuration file when exactly one supported argument is present
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
   * Resolves the CSV output file used for MySQL-backed self-checks.
   *
   * @param config parsed self-check configuration
   * @return configured CSV output path or a generated temporary file path
   */
  private def outputCsvFile(config: SelfCheckDuplicatedConfig): Try[String] =
    Try(config.outCsvFile.getOrElse(Files.createTempFile("SelfCheckDuplicated-", ".csv").toFile.getAbsolutePath))

  /**
   * Resolves the Lucene index path used by the self-check workflow.
   *
   * @param config parsed self-check configuration
   * @return configured Lucene index path or a generated temporary directory path
   */
  private def outputIndex(config: SelfCheckDuplicatedConfig): Try[String] =
    Try(config.index.getOrElse(Files.createTempDirectory("CSV2Lucene-").toFile.getAbsolutePath))

  /**
   * Prepares the configured producer as a CSV source.
   *
   * MySQL sources are exported to a generated CSV file and their schema is
   * derived from the header. CSV sources are reused directly from the
   * configuration.
   *
   * @param config parsed self-check configuration
   * @return prepared CSV source metadata
   */
  private def prepareSource(config: SelfCheckDuplicatedConfig): Try[PreparedCsvSource] =
    config.source match
      case SelfCheckMysqlSourceConfig(mysql) =>
        for
          csvFile <- outputCsvFile(config)
          _ = println(s"Generating CSV file: $csvFile...")
          _ <- SQL2CSV.exportRecords(mysql, csvFile)
          _ = println("CSV File generated.")
          schema <- schemaFromCsvHeader(csvFile, config.csvEncoding)
          _ <- SimilarDocs.requireSchemaFields(schema)
        yield PreparedCsvSource(
          csvFile = csvFile,
          schema = schema,
          hasHeader = true,
          fieldSeparator = ',',
          encoding = config.csvEncoding
        )

      case SelfCheckCsvSourceConfig(csv) =>
        SimilarDocs.requireSchemaFields(csv.schema).map:
          _ =>
            PreparedCsvSource(
              csvFile = csv.csvFile,
              schema = csv.schema,
              hasHeader = csv.hasHeader,
              fieldSeparator = csv.fieldSeparator,
              encoding = csv.encoding
            )

  /**
   * Derives a positional schema from the first non-empty CSV header line.
   *
   * @param csvFile CSV file whose header should be inspected
   * @param encoding character encoding used to read the CSV file
   * @return schema mapping CSV positions to field names
   */
  private def schemaFromCsvHeader(csvFile: String,
                                  encoding: String): Try[Map[Int, String]] =
    val header: Try[String] = Using(Source.fromFile(csvFile)(using Codec(encoding))):
      source =>
        source.getLines()
          .find(_.trim.nonEmpty)
          .getOrElse(throw IllegalArgumentException(s"Empty CSV file: $csvFile"))

    header.flatMap(parseCsvLine).flatMap:
      fields =>
        Try:
          if fields.isEmpty then throw IllegalArgumentException(s"Empty CSV header: $csvFile")
          fields.zipWithIndex.map:
            case (field, index) =>
              val normalized = field.trim
              if normalized.isEmpty then throw IllegalArgumentException(s"Empty CSV header field at position $index")
              index -> normalized
          .toMap

  /**
   * Parses one CSV line using RFC 4180-style quote handling.
   *
   * @param line CSV line to parse
   * @return parsed field values from the line
   */
  private def parseCsvLine(line: String): Try[Seq[String]] =
    Try:
      val fields: mutable.ReusableBuilder[String, Vector[String]] = Vector.newBuilder[String]
      val current: StringBuilder = new StringBuilder
      var inQuotes: Boolean = false
      var index: Int = 0

      while index < line.length do
        val char: Char = line.charAt(index)
        if inQuotes && char == '"' && index + 1 < line.length && line.charAt(index + 1) == '"' then
          current.append('"')
          index += 1
        else if char == '"' then inQuotes = !inQuotes
        else if char == ',' && !inQuotes then
          fields += current.toString()
          current.clear()
        else current.append(char)

        index += 1

      if inQuotes then throw IllegalArgumentException("Invalid CSV header: unclosed quote")
      fields += current.toString()
      fields.result()

  /**
   * Builds the command-line arguments required by CSV2Lucene.
   *
   * @param config parsed self-check configuration
   * @param source prepared CSV source to index
   * @param index Lucene index path to create
   * @return argument array accepted by CSV2Lucene.run
   */
  private def csv2LuceneArgs(config: SelfCheckDuplicatedConfig,
                             source: PreparedCsvSource,
                             index: String): Array[String] =
    val required = Array(
      s"-csvFile=${source.csvFile}",
      s"-index=$index",
      s"-schema=${schemaArgument(source.schema)}",
      s"-fieldToIndex=${config.searchField}",
      s"-fieldSeparator=${source.fieldSeparator}",
      s"-encoding=${source.encoding}"
    )
    if source.hasHeader then required :+ "--hasHeader" else required

  /**
   * Serializes a positional schema into the inline schema format accepted by CSV2Lucene.
   *
   * @param schema schema mapping CSV positions to field names
   * @return inline schema argument text
   */
  private def schemaArgument(schema: Map[Int, String]): String =
    schema.toSeq.sortBy(_._1).map:
      case (index, field) => s"$index=$field"
    .mkString(",")

  /**
   * Creates the SimilarDocs pipeline used by the self-check workflow.
   *
   * The finder points to the freshly built Lucene index and all configured
   * reporters are wrapped so self-pairs are ignored.
   *
   * @param config parsed self-check configuration
   * @param index Lucene index path created for this run
   * @return configured SimilarDocs instance
   */
  private def createSimilarDocs(config: SelfCheckDuplicatedConfig,
                                index: String): SimilarDocs =
    new SimilarDocs(
      SimilarDocsConfig(
        producer = new DocsProducer:
          override def getDocuments: LazyList[Document] = LazyList.empty,
        finder = LuceneDocsFinder(index, config.searchField, config.minSimilarity),
        comparators = config.comparators,
        reporters = config.reporters.map:
          configured =>
            ConfiguredReporter(SelfPairSkippingReporter(configured.reporter), configured.otherFields),
        auxQuery = config.auxQuery,
        maxDocs = config.maxDocs,
        documentParallelism = config.documentParallelism,
        heuristic = config.heuristic
      )
    )

  /**
   * Runs an operation and closes the SimilarDocs pipeline afterward.
   *
   * @param similarDocs SimilarDocs instance whose resources must be closed
   * @param operation operation to execute before closing resources
   * @return result of the operation, preserving close failures when appropriate
   */
  private def closeAfter(similarDocs: SimilarDocs)(operation: => Try[Unit]): Try[Unit] =
    val operationResult: Try[Unit] = Try(operation).flatten
    val closeResult: Try[Unit] = similarDocs.close()

    operationResult match
      case Success(_) => closeResult
      case Failure(exception) =>
        closeResult.failed.foreach(exception.addSuppressed)
        Failure(exception)

  /**
   * Metadata for the CSV source consumed by the self-check workflow.
   *
   * @param csvFile CSV file path
   * @param schema positional schema for the CSV file
   * @param hasHeader whether the CSV file contains a header row
   * @param fieldSeparator CSV field separator
   * @param encoding CSV file character encoding
   */
  private case class PreparedCsvSource(csvFile: String,
                                       schema: Map[Int, String],
                                       hasHeader: Boolean,
                                       fieldSeparator: Char,
                                       encoding: String)

  /**
   * Reporter wrapper that suppresses comparisons where both documents have the
   * same required identifiers.
   *
   * @param delegate reporter that receives non-self comparison results
   */
  private case class SelfPairSkippingReporter(delegate: Reporter) extends Reporter:
    /**
     * Writes comparison results unless the compared documents represent the same record.
     *
     * @param originalDoc source document used in the comparison
     * @param currentDoc candidate document being evaluated
     * @param otherFields additional field names to include in the output
     * @param results comparison results produced for the document pair
     * @return result of writing or skipping the comparison output
     */
    override def writeResults(originalDoc: Document,
                              currentDoc: Document,
                              otherFields: Seq[String],
                              results: Seq[CompResult]): Try[Unit] =
      if sameDocument(originalDoc, currentDoc) then Success(())
      else delegate.writeResults(originalDoc, currentDoc, otherFields, results)

    /**
     * Closes the wrapped reporter.
     *
     * @return result of closing the wrapped reporter
     */
    override def close(): Try[Unit] = delegate.close()

    /**
     * Checks whether two documents have the same required identifiers.
     *
     * @param originalDoc source document used in the comparison
     * @param currentDoc candidate document being evaluated
     * @return true when both documents have matching database and id fields
     */
    private def sameDocument(originalDoc: Document,
                             currentDoc: Document): Boolean =
      Seq("dbase", "id").forall:
        field =>
          firstField(originalDoc, field).exists(value => firstField(currentDoc, field).contains(value))

    /**
     * Returns the first value associated with a field in a document.
     *
     * @param document document to inspect
     * @param field field name to retrieve
     * @return first field value when present
     */
    private def firstField(document: Document,
                           field: String): Option[String] =
      document.fields.collectFirst:
          case (`field`, value) => value
