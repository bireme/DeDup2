package dd.producers

import dd.interfaces.{DocsProducer, Document}

import java.nio.charset.{CharsetDecoder, CodingErrorAction}
import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}
import play.api.libs.json.{JsArray, JsObject, JsString, JsValue, Json}

import scala.io.{BufferedSource, Codec, Source}
import scala.util.{Failure, Success, Try}

/**
 * Immutable configuration used by the MySQL-backed document producer.
 *
 * The configuration groups connection details, SQL source settings, and the
 * optional mappings needed to expand JSON and repetitive fields into the
 * internal document representation produced by the pipeline.
 */
case class MySqlProducerConfig(mySqlHost: String,
                               mySqlPort: Int,
                               mySqlDbname: String,
                               mySqlUser: String,
                               mySqlPassword: String,
                               sqlfs: Seq[String],
                               sqlEncoding: String,
                               jsonFields: Option[Map[String, Map[String, String]]],
                               repetitiveFields: Option[Set[String]],
                               repetitiveSep: Option[String]):
  require(mySqlHost.trim.nonEmpty)
  require(mySqlPort > 0)
  require(mySqlDbname.trim.nonEmpty)
  require(sqlfs.nonEmpty)
  require(sqlfs.forall(_.trim.nonEmpty))
  require(sqlEncoding.trim.nonEmpty)
  require(repetitiveFields.isEmpty || repetitiveSep.isDefined)

  def sqlf: String = sqlfs.mkString(",")

/**
 * Companion object for `MysqlProducer`.
 *
 * It contains shared type aliases, constants, and helper methods used to parse
 * JSON field mappings and expand SQL row values into document field variants.
 */
object MysqlProducer:
  private[producers] type FieldEntries = Seq[(String, String)]
  private[producers] type FieldVariants = Seq[FieldEntries]
  private val JsonArrayValueSeparator: String = "//@//"
  private val MysqlStreamingFetchSize: Int = Integer.MIN_VALUE

  /**
   * Parses one jsonFieldFile mapping line.
   *
   * A mapping can be written as `column=jsonField->newField` or as
   * `column=jsonField`. When the output field is omitted, the SQL column name is
   * used as the output field name.
   *
   * @param line raw mapping line
   * @return parsed mapping, or none for blank lines
   */
  private[dd] def parseJsonFieldMappingLine(line: String): Option[(String, String, String)] =
    val trimmed = line.trim
    if trimmed.isEmpty then None
    else
      val parts = trimmed.split(" *= *", 2)
      if parts.length != 2 || parts.exists(_.trim.isEmpty) then
        throw IllegalArgumentException(line)

      val column = parts(0).trim
      val mapping = parts(1).split(" *-> *", 2).map(_.trim)
      if mapping.isEmpty || mapping.head.isEmpty then
        throw IllegalArgumentException(line)

      val outputField =
        if mapping.length == 2 && mapping(1).nonEmpty then mapping(1)
        else column

      Some((column, mapping.head, outputField))

  /**
   * Extracts the configured JSON values from the current field.
   *
   * @param jsonStr raw JSON content to parse
   * @param jsonFieldName name assigned to values that cannot be expanded
   * @param jsonFields mapping that describes how JSON fields should be extracted
   * @return field variants extracted from the JSON content
   */
  private[producers] def getJsonSeq(jsonStr: String,
                                    jsonFieldName: String,
                                    jsonFields: Map[String, String]): Try[FieldVariants]=
    Try:
      jsonStr.trim match
        case "" => Seq(Seq(jsonFieldName -> ""))
        case jstr =>
          Try(Json.parse(jstr)).map:
            case arr: JsArray =>
              Seq(extractJsonArrayFields(arr, jsonFieldName, jsonFields))
            case obj: JsObject =>
              Seq(extractJsonObjectFields(obj, jsonFields))
            case str: JsString =>
              Seq(Seq(jsonFieldName -> str.toString()))
            case other =>
              throw new IllegalArgumentException(other.toString())
          .getOrElse(Seq(Seq(jsonFieldName -> jstr)))

  /**
   * Extracts configured JSON paths from the given JSON object.
   *
   * @param obj parsed JSON object used as the extraction source
   * @param jsonFields mapping between JSON paths and output field names
   * @return field entries extracted from the JSON object
   */
  private def extractJsonObjectFields(obj: JsObject,
                                      jsonFields: Map[String, String]): FieldEntries =
    jsonFields.toSeq.flatMap:
      case (path, newName) => (obj \ path).asOpt[String].map(newName -> _)

  /**
   * Extracts and groups configured JSON array values.
   *
   * Object arrays are extracted using the configured mappings and grouped by
   * output field name. Arrays of non-objects are grouped under the SQL column
   * name because there is no object field path to extract.
   *
   * @param arr parsed JSON array used as the extraction source
   * @param jsonFieldName name assigned to values that cannot be expanded
   * @param jsonFields mapping between JSON paths and output field names
   * @return grouped field entries extracted from the JSON array
   */
  private def extractJsonArrayFields(arr: JsArray,
                                     jsonFieldName: String,
                                     jsonFields: Map[String, String]): FieldEntries =
    val seq: Seq[JsValue] = arr.value.toSeq
    seq.headOption match
      case Some(_: JsObject) =>
        val entries: Seq[FieldEntries] = seq.collect:
          case obj: JsObject => extractJsonObjectFields(obj, jsonFields)
        entries.flatten.groupMap(_._1)(_._2).toSeq.map:
          case (field, values) => field -> joinJsonArrayValues(values)
      case Some(_) =>
        Seq(jsonFieldName -> joinJsonArrayValues(seq.map(_.toString())))
      case None =>
        Seq(jsonFieldName -> "")

  /**
   * Joins non-empty array values after case-insensitive sorting while preserving originals.
   *
   * @param values values extracted from the JSON array
   * @return sorted and grouped field value
   */
  private def joinJsonArrayValues(values: Seq[String]): String =
    values.map(_.trim).filter(_.nonEmpty).sortBy(_.toLowerCase).mkString(JsonArrayValueSeparator)

/**
 * Database-backed document producer that streams rows from MySQL.
 *
 * The producer executes the configured SQL query, converts each result row into
 * one or more internal documents, and expands JSON or repetitive fields when
 * the corresponding configuration options are present.
 *
 * @param conf MySQL connection, SQL source, encoding, JSON field, and
 *             repetitive field settings used by this producer
 */
class MysqlProducer(conf: MySqlProducerConfig) extends DocsProducer:
  import MysqlProducer.{FieldEntries, FieldVariants}

  val url = s"jdbc:mysql://${conf.mySqlHost.trim}:${conf.mySqlPort}/${conf.mySqlDbname.trim}?useTimezone=true&serverTimezone=UTC&useSSL=false"
  private val codec: Codec = conf.sqlEncoding.toLowerCase match
    case "iso8859-1" => scala.io.Codec.ISO8859
    case _           => scala.io.Codec.UTF8
  private val codAction: CodingErrorAction = CodingErrorAction.REPLACE
  private def decoder: CharsetDecoder = codec.decoder.onMalformedInput(codAction)

  /**
   * Returns the produced documents.
   * @return lazy list of produced documents
   */
  override def getDocuments: LazyList[Document] =
    conf.sqlfs.to(LazyList).flatMap(documentsFromSqlFile)

  /**
   * Returns the produced documents for one SQL file.
   *
   * @param sqlFile SQL file whose query will be executed
   * @return lazy list of produced documents
   */
  private def documentsFromSqlFile(sqlFile: String): LazyList[Document] =
    val con: Connection = DriverManager.getConnection(url, conf.mySqlUser, conf.mySqlPassword)
    con.setReadOnly(true)
    val statement: Statement = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    statement.setFetchSize(MysqlProducer.MysqlStreamingFetchSize)
    val reader: BufferedSource = Source.fromFile(sqlFile)(using Codec(decoder))
    val content: String =
      try reader.getLines().mkString(" ")
      finally reader.close()

    print(s"Executing query file: $sqlFile ... ")
    val rs: ResultSet = statement.executeQuery(content)
    println("OK")

    getDocuments(rs, statement, con, Seq.empty)

  /**
   * Returns the produced documents from the current result set.
   *
   * @param rs result set being streamed
   * @param statement statement that produced the result set
   * @param con database connection to close when the result set is exhausted
   * @param previous documents prepared in earlier recursive calls
   * @return lazy list of produced documents
   */
  private def getDocuments(rs: ResultSet,
                           statement: Statement,
                           con: Connection,
                           previous: Seq[Document]): LazyList[Document] = {
    previous match
      case head +: tail => head #:: getDocuments(rs, statement, con, tail)
      case _ =>
        if rs.next() then
          parseRecord(rs, conf.jsonFields, conf.repetitiveFields, conf.repetitiveSep) match
            case Success(doc +: tail) => doc #:: getDocuments(rs, statement, con, tail)
            case Success(_) => getDocuments(rs, statement, con, Seq.empty)
            case Failure(exception) =>
              //exception.printStackTrace()
              Console.err.println(s"MysqlProducer/getDocuments/${exception.getMessage}")
              getDocuments(rs, statement, con, previous)
              /*con.close()
              LazyList.empty*/
        else
          rs.close()
          statement.close()
          con.close()
          LazyList.empty
  }

  /**
   * Parses the current database record into internal documents.
   *
   * @param rs result set positioned at the current database row
   * @param jsonFields mapping that describes how JSON fields should be extracted
   * @param repetitiveFields field names that may produce repeated values
   * @param repetitiveSep value of repetitive sep
   * @return documents parsed from the current database row
   */
  private def parseRecord(rs: ResultSet,
                          jsonFields: Option[Map[String, Map[String, String]]],
                          repetitiveFields: Option[Set[String]],
                          repetitiveSep: Option[String]): Try[Seq[Document]] = {
    for
      fieldVariants <- fieldNames(rs).flatMap:
        _.foldLeft(Try(Seq(Seq.empty[(String, String)]))):
          case (acc, (column, fieldName)) =>
            for
              current <- acc
              values <- extractFieldValues(rs, column, fieldName, jsonFields, repetitiveFields, repetitiveSep)
            yield combineFieldVariants(current, values)
    yield fieldVariants.map(fields => Document(fields))
  }

  /**
   * Resolves the database column names for the current result set.
   *
   * @param rs result set positioned at the current database row
   * @return result containing the indexed column names
   */
  private def fieldNames(rs: ResultSet): Try[Seq[(Int, String)]] =
    Try:
      val meta: ResultSetMetaData = rs.getMetaData
      (1 to meta.getColumnCount).map(column => column -> meta.getColumnLabel(column))

  /**
   * Extracts the values associated with the current database column.
   *
   * @param rs result set positioned at the current database row
   * @param column database column index
   * @param fieldName logical field name associated with the column
   * @param jsonFields mapping used to expand JSON fields
   * @param repetitiveFields field names configured as repetitive
   * @param repetitiveSep separator used to split repetitive fields
   * @return result containing the normalized field variants for the column
   */
  private def extractFieldValues(rs: ResultSet,
                                 column: Int,
                                 fieldName: String,
                                 jsonFields: Option[Map[String, Map[String, String]]],
                                 repetitiveFields: Option[Set[String]],
                                 repetitiveSep: Option[String]): Try[FieldVariants] = {
    Option(rs.getString(column)).map(_.trim)
      .map:
        content =>
          jsonFields.flatMap(_.get(fieldName))
            .map(MysqlProducer.getJsonSeq(content, fieldName, _))
            .getOrElse(Success(splitRepetitive(fieldName, content, repetitiveFields, repetitiveSep).map(value => Seq(fieldName -> value))))
      .getOrElse(Success(Seq(Seq(fieldName -> ""))))
  }

  /**
   * Splits repetitive field content when configured for the selected field.
   *
   * @param fieldName logical field name associated with the current value
   * @param content raw column content to normalize
   * @param repetitiveFields field names configured as repetitive
   * @param repetitiveSep separator used to split repetitive fields
   * @return normalized sequence of field values
   */
  private def splitRepetitive(fieldName: String,
                              content: String,
                              repetitiveFields: Option[Set[String]],
                              repetitiveSep: Option[String]): Seq[String] =
    repetitiveFields match
      case Some(fields) if fields.contains(fieldName) => content.split(repetitiveSep.get).toSeq
      case _ => Seq(content)

  /**
   * Combines the current document variants with the variants emitted by a field.
   *
   * @param documents document variants accumulated so far
   * @param fieldVariants variants emitted by the current field
   * @return combined document variants
   */
  private def combineFieldVariants(documents: Seq[FieldEntries],
                                   fieldVariants: FieldVariants): Seq[FieldEntries] =
    val variants = if fieldVariants.isEmpty then Seq(Seq.empty) else fieldVariants
    for
      document <- documents
      fields <- variants
    yield document ++ fields
