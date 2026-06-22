package dd.reporters

import dd.NGAnalyzer
import dd.interfaces.{CompResult, Document, Reporter}
import org.apache.lucene.document
import org.apache.lucene.document.{Field, StoredField, TextField}
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.{Directory, FSDirectory}

import java.io.File
import java.nio.file.Path
import scala.util.{Failure, Success, Try}

/**
 * Reporter that stores comparison results in a Lucene index.
 *
 * The report is written as flat stored fields compatible with the pipe reporter
 * output, while the configured field is indexed with the shared NGAnalyzer so
 * it can be searched by n-gram similarity.
 */
class LuceneReporter(index: String,
                     fieldToIndex: String,
                     append: Boolean = false,
                     fieldNameMapping: Map[String, String] = Map.empty,
                     minTrue: Int = 0) extends Reporter:
  private val indexPath: Path = new File(index).toPath
  private val directory: Directory = FSDirectory.open(indexPath)
  private val analyzer: NGAnalyzer = new NGAnalyzer()
  private val openMode: IndexWriterConfig.OpenMode =
    if append then IndexWriterConfig.OpenMode.CREATE_OR_APPEND
    else IndexWriterConfig.OpenMode.CREATE
  private val config: IndexWriterConfig = new IndexWriterConfig(analyzer).setOpenMode(openMode)
  private val writer: IndexWriter = new IndexWriter(directory, config)

  /**
   * Writes the comparison results.
   *
   * @param originalDoc source document used in the comparison
   * @param currentDoc candidate document being evaluated
   * @param otherFields additional field names to include in the output
   * @param results comparison results produced for the document pair
   * @return result of writing the comparison output
   */
  override def writeResults(originalDoc: Document,
                            currentDoc: Document,
                            otherFields: Seq[String],
                            results: Seq[CompResult]): Try[Unit] =
    if LuceneReporter.shouldWriteResults(results, minTrue) then
      val reportFields = LuceneReporter.reportFields(originalDoc, currentDoc, otherFields, results, fieldNameMapping)
      addDocument(reportFields)
    else Success(())

  /**
   * Closes the underlying resources.
   * @return result of closing the underlying resources
   */
  override def close(): Try[Unit] =
    val closedWriter = Try:
      writer.forceMerge(1)
      writer.close()
    val closedAnalyzer = Try(analyzer.close())
    val closedDirectory = Try(directory.close())

    Seq(closedWriter, closedAnalyzer, closedDirectory).collectFirst:
      case Failure(exception) => Failure(exception)
    .getOrElse(Success(()))

  /**
   * Adds one Lucene document for the current comparison.
   *
   * @param fields report fields to store
   * @return result of adding the document to the Lucene index
   */
  private def addDocument(fields: Seq[(String, String)]): Try[Unit] =
    Try:
      val luceneDocument = fields.foldLeft(new document.Document()):
        case (doc, (name, value)) =>
          if name == fieldToIndex then doc.add(new TextField(name, value, Field.Store.YES))
          else doc.add(new StoredField(name, value))
          doc

      require(fields.exists(_._1 == fieldToIndex), s"Field to index [$fieldToIndex] is not present in the report fields.")
      writer.addDocument(luceneDocument)

private[reporters] object LuceneReporter:
  private val fieldSeparator: String = "|"

  /**
   * Checks whether a comparison result set satisfies the reporter threshold.
   *
   * @param results comparison results produced for the current document pair
   * @param minTrue minimum number of successful comparator results required
   * @return true when the result set should be written
   */
  def shouldWriteResults(results: Seq[CompResult], minTrue: Int): Boolean =
    results.count(_.isSimilar) >= minTrue

  /**
   * Builds the flat field list stored in the Lucene report document.
   *
   * @param originalDoc source document used in the comparison
   * @param currentDoc candidate document being evaluated
   * @param otherFields additional field names included in the output
   * @param results comparison results produced for the document pair
   * @param fieldNameMapping optional field-name replacements applied before storing
   * @return report fields ready to be added to a Lucene document
   */
  def reportFields(originalDoc: Document,
                   currentDoc: Document,
                   otherFields: Seq[String],
                   results: Seq[CompResult],
                   fieldNameMapping: Map[String, String] = Map.empty): Seq[(String, String)] =
    val baseFields = otherFields.flatMap:
      field =>
        Seq(
          s"${field.trim}_1" -> collectField(originalDoc, field),
          s"${field.trim}_2" -> collectField(currentDoc, field)
        )

    (baseFields ++ results.flatMap(resultFields)).map:
      case (fieldName, value) =>
        fieldNameMapping.getOrElse(fieldName, fieldName) -> nullIfEmpty(value)

  /**
   * Returns all values for a requested field as a single stored value.
   *
   * @param doc document that provides the field values
   * @param fieldName field name to collect
   * @return joined field value, or the empty string when absent
   */
  private def collectField(doc: Document,
                           fieldName: String): String =
    doc.fields.filter(_._1 == fieldName).map(_._2).mkString(fieldSeparator)

  /**
   * Returns the flat fields for a comparison result.
   *
   * @param result comparison result to serialize
   * @return flat Lucene fields derived from the comparison result
   */
  private def resultFields(result: CompResult): Seq[(String, String)] =
    Seq(
      "Comparator" -> result.name,
      "Field" -> result.fieldName,
      "originalField" -> result.originalField,
      "currentField" -> result.currentField,
      "originalFieldOther" -> result.originalFieldOther.getOrElse(""),
      "currentFieldOther" -> result.currentFieldOther.getOrElse(""),
      "Similarity" -> result.similarity.toString,
      "isSimilar" -> ReporterSimilarityStatus.displayValue(result)
    )

  /**
   * Converts empty output cells to the textual null marker.
   *
   * @param value serialized field value
   * @return original value or "null" when empty
   */
  private def nullIfEmpty(value: String): String =
    if value.isEmpty then "null" else value
