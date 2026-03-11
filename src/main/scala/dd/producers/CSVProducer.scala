package dd.producers

import dd.interfaces.{DocsProducer, Document}
import org.apache.commons.csv.{CSVFormat, CSVParser, CSVRecord}

import java.io.InputStreamReader
import scala.io.Source
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.{Failure, Success, Try, Using}

/**
 * CSV-backed document producer used by the processing pipeline.
 *
 * The producer streams CSV records lazily, applies the configured positional
 * schema, and converts each valid record into the internal `Document` model
 * while skipping malformed entries with diagnostic messages.
 */
class CSVProducer(csvFile: String,
                  schema: Map[Int,String],
                  hasHeader: Boolean,
                  fieldSeparator: Char =',',
                  csvFileEncoding: String = "utf-8") extends DocsProducer:
  /**
   * Returns the produced documents.
   * @return lazy list of produced documents
   */
  override def getDocuments: LazyList[Document] =
    Try:
      val formatBuilder: CSVFormat.Builder = CSVFormat.Builder.create().setDelimiter(fieldSeparator).setTrim(true)
        .setSkipHeaderRecord(hasHeader).setIgnoreEmptyLines(true).setQuote(null)
      val reader: InputStreamReader = Source.fromFile(csvFile, csvFileEncoding).reader()
      val parser: CSVParser = CSVParser.builder().setFormat(formatBuilder.get()).setReader(reader).get()
      val records: Iterator[CSVRecord] =
        if hasHeader then parser.iterator().asScala.drop(1)
        else parser.iterator().asScala

      getDocumentsLazy(parser, schema, records)
    match
      case Success(list) => list
      case Failure(exception: Throwable) =>
        Console.err.println(s"CSVProducer/getDocuments/${exception.getMessage}")
        LazyList.empty[Document]

  /**
   * Builds the lazy list of produced documents.
   *
   * @param parser CSV parser that provides the records
   * @param schema mapping between positions and field names
   * @param iterator remaining CSV records to transform
   * @return lazy list of produced documents
   */
  private def getDocumentsLazy(parser: CSVParser,
                               schema: Map[Int,String],
                               iterator: Iterator[CSVRecord]): LazyList[Document] =
    nextDocument(schema, iterator) match
      case Some(document) => document #:: getDocumentsLazy(parser, schema, iterator)
      case None =>
        closeParser(parser)
        LazyList.empty

  /**
   * Reads the next valid document from the CSV iterator.
   *
   * @param schema mapping between positions and field names
   * @param iterator remaining CSV records to transform
   * @return next successfully converted document when available
   */
  private def nextDocument(schema: Map[Int, String],
                           iterator: Iterator[CSVRecord]): Option[Document] =
    if !iterator.hasNext then None
    else
      buildDocument(schema, iterator.next()) match
        case Success(doc) => Some(doc)
        case Failure(exception) =>
          Console.err.println(s"CSVProducer/getDocumentsLazy/${exception.getMessage}")
          nextDocument(schema, iterator)

  /**
   * Converts a CSV record into the internal document representation.
   *
   * @param schema mapping between positions and field names
   * @param record CSV record currently being processed
   * @return result containing the converted document
   */
  private def buildDocument(schema: Map[Int, String],
                            record: CSVRecord): Try[Document] =
    Try:
      val recSize: Int = record.size()
      val fields: Seq[(String, String)] = schema.toSeq.sortBy(_._1).map:
        case (pos, field) =>
          require(pos < recSize,
            s"Invalid position. Record size=$recSize Position=$pos. Please, compare the schema position field with cvs number of fields.")
          field -> record.get(pos)
      Document(fields)

  /**
   * Closes the CSV parser once the lazy stream has been fully consumed.
   *
   * @param parser parser instance associated with the current input stream
   * @return no value; this method closes the parser for cleanup
   */
  private def closeParser(parser: CSVParser): Unit =
    Using(parser)(_.close())
