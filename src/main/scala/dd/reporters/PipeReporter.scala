package dd.reporters

import dd.interfaces.{CompResult, Document, Reporter}
import org.apache.commons.csv.CSVFormat

import java.io.Writer
import scala.util.Try

/**
 * Reporter that serializes comparison results as delimited text output.
 *
 * The reporter optionally emits a header row, filters matches according to the
 * minimum number of successful comparator results, and writes the selected
 * fields plus comparison metadata into a pipe-separated stream.
 */
class PipeReporter(writer: Writer,
                   recordSeparator: String,
                   putHeader: Boolean,
                   minTrue: Int) extends Reporter:
  private val fieldSeparator: String = "|"
  private val formatBuilder: CSVFormat.Builder = CSVFormat.Builder.create().setRecordSeparator(recordSeparator).setTrim(true)
    .setDelimiter(fieldSeparator).setAutoFlush(true)
  private val format: CSVFormat = formatBuilder.get()
  private var headerWritten: Boolean = !putHeader
  private var rowWritten: Boolean = false

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
    for
      _ <- requireNonEmpty(results)
      _ <- ensureHeader(otherFields, results)
      _ <- writeRowIfEligible(originalDoc, currentDoc, otherFields, results)
    yield ()

  /**
   * Closes the underlying resources.
   * @return result of closing the underlying resources
   */
  override def close(): Try[Unit] = Try(writer.close())

  /**
   * Writes the output header row.
   *
   * @param format CSV format used to serialize the output
   * @param otherFields additional field names to include in the output
   * @param results comparison results produced for the document pair
   * @return error message or unit result for the header write
   */
  private def writeHeader(format: CSVFormat,
                          otherFields: Seq[String],
                          results: Seq[CompResult]): Try[Unit] =
    val header = otherFields.flatMap(fld => Seq(s"${fld.trim}_1", s"${fld.trim}_2")) ++
      results.flatMap(_ => Seq("Comparator", "Field", "Content1", "Content2", "Similarity", "isSimilar"))

    Try(writer.write(format.format(header*)))

  /**
   * Returns the selected extra fields for both documents.
   *
   * @param originalDoc source document used in the comparison
   * @param currentDoc candidate document being evaluated
   * @param otherFields additional field names to include in the output
   * @return serialized extra fields for both documents
   */
  private def getOtherFields(originalDoc: Document,
                             currentDoc: Document,
                             otherFields: Seq[String]): (Seq[String], Seq[String]) =
    /**
     * Collects the requested field values from the document.
     *
     * @param doc document to insert or serialize
     * @return serialized field values extracted from the document
     */
    def collectFields(doc: Document): Seq[String] =
      otherFields.flatMap:
        oField =>
          val seq: Seq[String] = doc.fields.filter(_._1.equals(oField)).map(_._2)
          Option.when(seq.nonEmpty)(seq.mkString(fieldSeparator))

    (collectFields(originalDoc), collectFields(currentDoc))

  private def requireNonEmpty(results: Seq[CompResult]): Try[Unit] =
    Try:
      require(results.nonEmpty, "Empty results sequence")

  /**
   * Ensures the output header has been written before any data row.
   *
   * @param otherFields additional field names included in the output
   * @param results comparison results produced for the current document pair
   * @return result of guaranteeing the header presence
   */
  private def ensureHeader(otherFields: Seq[String],
                           results: Seq[CompResult]): Try[Unit] =
    if headerWritten then Try(())
    else
      writeHeader(format, otherFields, results).map(_ => headerWritten = true)

  /**
   * Writes the current row when it satisfies the configured threshold.
   *
   * @param originalDoc source document used in the comparison
   * @param currentDoc candidate document being evaluated
   * @param otherFields additional field names included in the output
   * @param results comparison results produced for the current document pair
   * @return result of writing the serialized row when eligible
   */
  private def writeRowIfEligible(originalDoc: Document,
                                 currentDoc: Document,
                                 otherFields: Seq[String],
                                 results: Seq[CompResult]): Try[Unit] =
    if results.count(_.isSimilar) < minTrue then Try(())
    else
      val serialized = serializeRow(originalDoc, currentDoc, otherFields, results)
      Try:
        if rowWritten then writer.write("\n")
        writer.write(format.format(serialized*))
        rowWritten = true

  /**
   * Serializes the current comparison into an output row.
   *
   * @param originalDoc source document used in the comparison
   * @param currentDoc candidate document being evaluated
   * @param otherFields additional field names included in the output
   * @param results comparison results produced for the current document pair
   * @return serialized row values ready to be written
   */
  private def serializeRow(originalDoc: Document,
                           currentDoc: Document,
                           otherFields: Seq[String],
                           results: Seq[CompResult]): Seq[String] =
    val (originalFields, currentFields) = getOtherFields(originalDoc, currentDoc, otherFields)
    originalFields ++ currentFields ++ results.flatMap(getResultFields)

  /**
   * Returns the serialized values of a comparison result.
   *
   * @param result comparison result to serialize
   * @return serialized representation of the comparison result
   */
  private def getResultFields(result: CompResult): Seq[String] =
    Seq(result.name, result.fieldName,
      result.originalFieldOther.getOrElse(result.originalField),
      result.currentFieldOther.getOrElse(result.currentField),
      result.similarity.toString, result.isSimilar.toString)
