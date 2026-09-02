package dd.reporters

import dd.interfaces.{CompResult, Document, Reporter}
import play.api.libs.json.{JsArray, JsNull, JsNumber, JsObject, JsString, JsValue, Json}

import java.io.Writer
import scala.util.Try

/**
 * Reporter that serializes comparison results as JSON output.
 *
 * The reporter writes a top-level JSON array incrementally. Each eligible
 * document pair is emitted as one object containing the selected extra fields
 * from both documents and the comparison result metadata.
 *
 * @param writer output writer that receives the serialized JSON array
 * @param minTrue minimum number of comparator results marked as similar
 *                required for a document pair to be written
 * @param flushResults true when the writer should be flushed after each
 *                     emitted document pair
 */
class JsonReporter(writer: Writer,
                   minTrue: Int,
                   flushResults: Boolean = false) extends Reporter:
  private val RecordSeparator: String = "\n"
  private var arrayStarted: Boolean = false
  private var rowWritten: Boolean = false
  private var closed: Boolean = false

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
                            results: Seq[CompResult]): Try[Unit] = {
    for
      _ <- requireNonEmpty(results)
      _ <- writeRowIfEligible(originalDoc, currentDoc, otherFields, results)
    yield ()
  }

  /**
   * Closes the JSON array and underlying writer.
   *
   * @return result of closing the output resources
   */
  override def close(): Try[Unit] =
    Try:
      closeArray()
      writer.close()

  /**
   * Validates that a report row has comparison results.
   *
   * @param results comparison results produced for the current document pair
   * @return successful result when the sequence is non-empty
   */
  private def requireNonEmpty(results: Seq[CompResult]): Try[Unit] =
    Try:
      require(results.nonEmpty, "Empty results sequence")

  /**
   * Writes the current JSON object when it satisfies the configured threshold.
   *
   * @param originalDoc source document used in the comparison
   * @param currentDoc candidate document being evaluated
   * @param otherFields additional field names included in the output
   * @param results comparison results produced for the current document pair
   * @return result of writing the serialized object when eligible
   */
  private def writeRowIfEligible(originalDoc: Document,
                                 currentDoc: Document,
                                 otherFields: Seq[String],
                                 results: Seq[CompResult]): Try[Unit] = {
    // if results.count(_.isSimilar) >= minTrue then println(s"isSimilar count=${results.count(_.isSimilar)} min=$minTrue")
    if results.count(_.isSimilar) < minTrue then Try(())
    else
      Try:
        ensureArrayStarted()
        if rowWritten then writer.write(s",$RecordSeparator")
        writer.write(Json.stringify(serializeRow(originalDoc, currentDoc, otherFields, results)))
        rowWritten = true
        flushIfRequested()
  }

  /**
   * Starts the top-level JSON array when the first row is written.
   */
  private def ensureArrayStarted(): Unit =
    if !arrayStarted then
      writer.write("[")
      writer.write(RecordSeparator)
      arrayStarted = true

  /**
   * Finishes the top-level JSON array exactly once.
   */
  private def closeArray(): Unit =
    if !closed then
      if !arrayStarted then writer.write("[]")
      else
        if rowWritten then writer.write(RecordSeparator)
        writer.write("]")
      closed = true

  /** Flushes the writer when immediate output was requested. */
  private def flushIfRequested(): Unit =
    if flushResults then writer.flush()

  /**
   * Serializes the current comparison into one JSON object.
   *
   * @param originalDoc source document used in the comparison
   * @param currentDoc candidate document being evaluated
   * @param otherFields additional field names included in the output
   * @param results comparison results produced for the current document pair
   * @return JSON object ready to be written
   */
  private def serializeRow(originalDoc: Document,
                           currentDoc: Document,
                           otherFields: Seq[String],
                           results: Seq[CompResult]): JsObject =
    Json.obj(
      "fields" -> JsObject(getOtherFields(originalDoc, currentDoc, otherFields)),
      "results" -> JsArray(results.map(serializeResult))
    )

  /**
   * Returns the selected extra fields for both documents.
   *
   * @param originalDoc source document used in the comparison
   * @param currentDoc candidate document being evaluated
   * @param otherFields additional field names to include in the output
   * @return JSON values for both sides of each requested field
   */
  private def getOtherFields(originalDoc: Document,
                             currentDoc: Document,
                             otherFields: Seq[String]): Seq[(String, JsValue)] =
    /** Collects all occurrences of a document field as JSON. */
    def collectField(doc: Document, oField: String): JsValue =
      nullIfEmpty(doc.fields.filter(_._1.equals(oField)).map(_._2).mkString("|"))

    otherFields.map:
      oField =>
        oField -> Json.obj(
          "original" -> collectField(originalDoc, oField),
          "current" -> collectField(currentDoc, oField)
        )

  /**
   * Serializes one comparison result into JSON.
   *
   * @param result comparison result to serialize
   * @return JSON representation of the comparison result
   */
  private def serializeResult(result: CompResult): JsObject =
    Json.obj(
      "comparator" -> result.name,
      "field" -> result.fieldName,
      "originalField" -> nullIfEmpty(result.originalField),
      "currentField" -> nullIfEmpty(result.currentField),
      "originalFieldOther" -> result.originalFieldOther.filter(_.nonEmpty).map(JsString.apply).getOrElse(JsNull),
      "currentFieldOther" -> result.currentFieldOther.filter(_.nonEmpty).map(JsString.apply).getOrElse(JsNull),
      "similarity" -> JsNumber(BigDecimal(result.similarity)),
      "isSimilar" -> similarityValue(result)
    )

  /** Converts a comparison result similarity to JSON. */
  private def similarityValue(result: CompResult): JsValue =
    JsString(ReporterSimilarityStatus.displayValue(result))

  /**
   * Converts empty output values to JSON null.
   *
   * @param value serialized field value
   * @return JSON string value or null when empty
   */
  private def nullIfEmpty(value: String): JsValue =
    if value.isEmpty then JsNull else JsString(value)
