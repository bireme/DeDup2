package dd

import dd.configurators.ConfMain
import dd.interfaces.{CompResult, Comparator, DocsFinder, Document, Reporter}
import dd.producers.CSVProducer

import java.io.File
import scala.util.{Failure, Success, Try}

/**
 * Core similarity-processing pipeline for source documents.
 *
 * The class coordinates candidate retrieval, comparator execution, and result
 * reporting for each input document while keeping the finder, comparators, and
 * reporters decoupled from the orchestration logic.
 */
class SimilarDocs(finder: DocsFinder,
                  filters: Seq[Comparator],
                  reporters: Seq[Reporter],
                  auxQuery: Option[String],
                  maxDocs: Int,
                  otherFields: Seq[String]):

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
  def close(): Try[Unit] = Try:
    finder.close().get
    reporters.foreach(_.close().get)

  /**
   * Finds and evaluates documents similar to the given source document.
   *
   * @param originalDoc source document used in the comparison
   * @return lazy list of matched documents and their comparison results
   */
  private def similar(originalDoc: Document): Try[LazyList[(Document, Seq[CompResult])]] =
    for
      searchField <- finder.getSearchField.toRight(IllegalArgumentException("Empty search field")).toTry
      query <- originalDoc.fields.collectFirst:
        case (`searchField`, value) => value
      .toRight(IllegalArgumentException("Empty search field")).toTry
      producer <- finder.findDocs(query, auxQuery, maxDocs)
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
    reporters.foldLeft(Try(())):
      case (acc, reporter) =>
        acc.flatMap(_ => reporter.writeResults(originalDoc, currentDoc, otherFields, results))

/**
 * Command-line entrypoint for the generic similarity-processing pipeline.
 *
 * This object parses the runtime arguments, loads the external configuration,
 * streams the input documents from CSV, and delegates the processing work to
 * the shared `SimilarDocs` orchestration class.
 */
object SimilarDocs:
  /**
   * Prints the command usage information and exits.
   * @return no value; this method terminates the application
   */
  private val usageMessage: String =
    """Check for duplicated documents in a database/index.
      |
      |usage: SimilarDocs <options>
      |
      |<options>:
      |	-inputPipeFile=<path> Input pipe document file. Contain documents(one per line) used to look for similar ones.
      |	-schema=<pos>:<fieldName>,...,<pos>:<fieldName> Associate the csv field position (starting from 0)
      |		with the Lucene document field's name. Only the fields present in the confFile.
      |	-confFile=<path>      Configuration file. See documentation for configuration file description.
      |	[-otherFields=<field1>,<field2>,...,<fieldN>]  Field names  not present in the schema but that
      |		should be included in the output report.
      |	[-auxQuery=<str>]     Auxiliary query used to complement the one used from searchField.
      |	[-maxDocs=<num>]      Maximum number of similar documents to be retrived. Default value is 100.
      |	[-encoding=<codec>]   Encoding of the input pipe file. Default value is utf-8.
      |	[-fieldSep=<char>]    Character used to separate the fields of the pipe file. Default value is ´|´
      |	[--hasHeader]         If present, it will skip the first line of the csv file""".stripMargin

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

  /**
   * Executes the command-line workflow with the provided arguments.
   *
   * @param args command-line arguments received by the application
   * @return result of the complete similarity-processing run
   */
  private def run(args: Array[String]): Try[Unit] =
    for
      parameters <- parseArgs(args)
      _ <- requireParameters(parameters, "inputPipeFile", "schema", "confFile")
      configured <- ConfMain.parseConfig(new File(parameters("confFile")))
      (finder, filters, reporters) = configured
      schema <- parseSchema(parameters("schema"))
      encoding = parameters.getOrElse("encoding", "utf-8")
      fieldSep = parameters.getOrElse("fieldSep", "|").headOption.getOrElse('|')
      docProducer = new CSVProducer(parameters("inputPipeFile"), schema, parameters.contains("hasHeader"), fieldSeparator = fieldSep, encoding)
      auxQuery = parameters.get("auxQuery")
      maxDocs = parameters.getOrElse("maxDocs", "100").toInt
      otherFields = parameters.getOrElse("otherFields", "").trim.split(" *, *").map(_.trim).filter(_.nonEmpty).toSeq
      similarDocs = new SimilarDocs(finder, filters, reporters, auxQuery, maxDocs, otherFields)
      _ <- docProducer.getDocuments.foldLeft(Try(())):
        case (acc, document) => acc.flatMap(_ => similarDocs.processSimilars(document))
      _ <- similarDocs.close()
    yield ()

  /**
   * Parses command-line arguments into a name/value map.
   *
   * @param args command-line arguments received by the application
   * @return parsed parameter map
   */
  private def parseArgs(args: Array[String]): Try[Map[String, String]] =
    Success(args.foldLeft(Map.empty[String, String]):
      case (map, par) =>
        val split = par.split(" *= *", 2)
        split.size match
          case 1 => map + (split(0).substring(2) -> "")
          case 2 => map + (split(0).substring(1) -> split(1))
          case _ => map
    )

  /**
   * Validates whether the required parameters are present.
   *
   * @param parameters parsed command-line parameter map
   * @param required parameter names that must be available
   * @return result indicating whether the validation succeeded
   */
  private def requireParameters(parameters: Map[String, String],
                                required: String*): Try[Unit] =
    val missing = required.filterNot(parameters.contains)
    if missing.isEmpty then Success(())
    else Failure(IllegalArgumentException(usageMessage))

  /**
   * Parses the schema argument into a positional field mapping.
   *
   * @param rawSchema raw schema string received from the command line
   * @return result containing the parsed schema map
   */
  private def parseSchema(rawSchema: String): Try[Map[Int, String]] =
    Try:
      rawSchema.trim.split(" *, *").iterator.filter(_.nonEmpty).map(_.trim).map:
        _.split(" *: *", 2)
      .map:
        case Array(index, field) => index.trim.toInt -> field.trim
        case other => throw IllegalArgumentException(s"Invalid schema entry: ${other.mkString(":")}")
      .toMap
