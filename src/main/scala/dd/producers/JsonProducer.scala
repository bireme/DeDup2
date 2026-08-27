package dd.producers

import dd.interfaces.{DocsProducer, Document}
import play.api.libs.json.{JsArray, JsBoolean, JsNull, JsNumber, JsObject, JsString, JsValue, Json}

import java.nio.file.{Files, Path}
import scala.annotation.tailrec
import scala.io.Source
import scala.util.{Failure, Success, Try, Using}

/**
 * JSON-backed document producer used by the processing pipeline.
 *
 * The input parameter can be either a JSON file path or a raw JSON string. JSON
 * arrays are streamed as one document per object; a single JSON object is
 * emitted as one document. Optional field mappings use the
 * `outputField=json.path` format and preserve the configured output order.
 *
 * @param input JSON file path or raw JSON content
 * @param fields optional output field mappings in `outputField=json.path` format
 * @param encoding character encoding used when input points to a file
 */
class JsonProducer(input: String,
                   fields: Option[Seq[String]] = None,
                   encoding: String = "utf-8") extends DocsProducer:
  private val fieldMappings: Option[Seq[JsonProducer.FieldMapping]] =
    fields.map(_.map(JsonProducer.parseFieldMapping(_).get))

  /**
   * Returns the produced documents.
   * @return lazy list of produced documents
   */
  override def getDocuments: LazyList[Document] =
    Try:
      val json = Json.parse(JsonProducer.readInput(input, encoding))
      val values = json match
        case JsArray(values) => values.iterator
        case obj: JsObject => Iterator.single(obj)
        case other => throw IllegalArgumentException(s"Expected JSON object or array, got: ${Json.stringify(other)}")

      getDocumentsLazy(values)
    match
      case Success(list) => list
      case Failure(exception) =>
        Console.err.println(s"JsonProducer/getDocuments/${exception.getMessage}")
        LazyList.empty[Document]

  /**
   * Builds the lazy list of produced documents.
   *
   * @param iterator remaining JSON values to transform
   * @return lazy list of produced documents
   */
  private def getDocumentsLazy(iterator: Iterator[JsValue]): LazyList[Document] =
    nextDocument(iterator) match
      case Some(document) => document #:: getDocumentsLazy(iterator)
      case None => LazyList.empty

  /**
   * Reads the next valid document from the JSON iterator.
   *
   * @param iterator remaining JSON values to transform
   * @return next successfully converted document when available
   */
  @tailrec
  private def nextDocument(iterator: Iterator[JsValue]): Option[Document] =
    if !iterator.hasNext then None
    else
      iterator.next() match
        case obj: JsObject =>
          JsonProducer.toDocument(obj, fieldMappings) match
            case Success(document) => Some(document)
            case Failure(exception) =>
              Console.err.println(s"JsonProducer/getDocumentsLazy/${exception.getMessage}")
              nextDocument(iterator)
        case other =>
          Console.err.println(s"JsonProducer/getDocumentsLazy/Expected JSON object, got: ${Json.stringify(other)}")
          nextDocument(iterator)

/**
 * Helper functions used by JsonProducer and its tests.
 */
private[producers] object JsonProducer:
  val DefaultFieldSeparator = "¦"

  /**
   * Mapping between an output document field and a JSON path.
   *
   * @param fieldName field name emitted in the internal Document
   * @param path JSON field path split into path segments
   */
  case class FieldMapping(fieldName: String,
                          path: Seq[String])

  /**
   * Reads the input as a file when it points to an existing path, otherwise
   * returns it as raw JSON content.
   *
   * @param input JSON file path or raw JSON content
   * @param encoding file character encoding
   * @return raw JSON content
   */
  private def readInput(input: String,
                        encoding: String): String =
    val maybePath = Try(Path.of(input)).toOption
    maybePath.filter(Files.exists(_)) match
      case Some(path) =>
        Using(Source.fromFile(path.toFile, encoding))(_.mkString).get
      case None =>
        input

  /**
   * Converts a JSON object into the internal document representation.
   *
   * When field mappings are provided, only mapped values are emitted and the
   * configured order is preserved. Without mappings, all top-level JSON fields
   * are emitted using their original names.
   *
   * @param obj JSON object to convert
   * @param fields optional configured field mappings
   * @return converted internal document, or a failure when conversion fails
   */
  def toDocument(obj: JsObject,
                 fields: Option[Seq[FieldMapping]]): Try[Document] =
    Try:
      val selectedFields = fields.getOrElse:
        obj.fields.map:
          case (field, _) => FieldMapping(field, Seq(field))
        .toVector

      val outputFields = selectedFields.map:
        mapping =>
          mapping.fieldName -> resolvePath(obj, mapping.path).map(valuesToField).getOrElse("")

      Document(outputFields.toVector)

  /**
   * Parses one configured JSON field mapping.
   *
   * The accepted syntax is `outputField=json.path`, where the right-hand side
   * can contain one or more dot-separated JSON path segments.
   *
   * @param raw raw mapping entry from configuration
   * @return parsed field mapping, or a failure for invalid syntax
   */
  def parseFieldMapping(raw: String): Try[FieldMapping] =
    Try:
      raw.split(" *= *", 2) match
        case Array(fieldName, path) =>
          val normalizedFieldName = fieldName.trim
          val normalizedPath = path.trim.split(" *\\. *").toSeq.map(_.trim).filter(_.nonEmpty)

          if normalizedFieldName.isEmpty then throw IllegalArgumentException(s"Empty output field in mapping: $raw")
          if normalizedPath.isEmpty then throw IllegalArgumentException(s"Empty JSON field path in mapping: $raw")

          FieldMapping(normalizedFieldName, normalizedPath)
        case _ =>
          throw IllegalArgumentException(s"Invalid JSON field mapping: $raw")

  /**
   * Resolves a dot-separated path against a JSON value.
   *
   * Arrays are traversed by applying the same remaining path to every element,
   * allowing mappings such as `author=authors.name` to collect a subfield from
   * every object in an array.
   *
   * @param value current JSON value being inspected
   * @param path remaining JSON path segments to resolve
   * @return resolved values when the path exists, otherwise none
   */
  private def resolvePath(value: JsValue,
                          path: Seq[String]): Option[Seq[JsValue]] =
    path match
      case Seq() => Some(Seq(value))
      case segment +: tail =>
        value match
          case obj: JsObject => (obj \ segment).toOption.flatMap(resolvePath(_, tail))
          case JsArray(values) =>
            val resolved = values.flatMap(item => resolvePath(item, path).getOrElse(Seq.empty))
            Some(resolved.toVector)
          case _ => None

  /**
   * Serializes one or more resolved values into a single document field value.
   *
   * Multiple values are joined with the default field separator so arrays do not
   * expand into repeated internal fields.
   *
   * @param values resolved JSON values
   * @return serialized field value
   */
  private def valuesToField(values: Seq[JsValue]): String =
    val strings = values.flatMap(valueToStrings)
    strings.mkString(DefaultFieldSeparator)

  /**
   * Converts an arbitrary JSON value into string values.
   *
   * Nested JSON objects are preserved as compact JSON. Arrays are flattened
   * recursively so the caller can join them with the configured field separator.
   *
   * @param value JSON value to serialize
   * @return string representation values derived from the input
   */
  private def valueToStrings(value: JsValue): Seq[String] =
    value match
      case JsNull => Seq("")
      case JsString(value) => Seq(value)
      case JsNumber(value) => Seq(value.toString)
      case JsBoolean(value) => Seq(value.toString)
      case obj: JsObject => Seq(Json.stringify(obj))
      case JsArray(values) => values.flatMap(valueToStrings).toSeq
