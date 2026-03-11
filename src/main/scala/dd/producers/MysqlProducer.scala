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
                               sqlf: String,
                               sqlEncoding: String,
                               jsonFields: Option[Map[String, Map[String, String]]],
                               repetitiveFields: Option[Set[String]],
                               repetitiveSep: Option[String]):
  require(mySqlHost.trim.nonEmpty)
  require(mySqlPort > 0)
  require(mySqlDbname.trim.nonEmpty)
  require(sqlf.trim.nonEmpty)
  require(sqlEncoding.trim.nonEmpty)
  require(repetitiveFields.isEmpty || repetitiveSep.isDefined)

/**
 * Database-backed document producer that streams rows from MySQL.
 *
 * The producer executes the configured SQL query, converts each result row into
 * one or more internal documents, and expands JSON or repetitive fields when
 * the corresponding configuration options are present.
 */
class MysqlProducer(conf: MySqlProducerConfig) extends DocsProducer:
  private val con: Connection = DriverManager.getConnection(
    s"jdbc:mysql://${conf.mySqlHost.trim}:${conf.mySqlPort}/${conf.mySqlDbname.trim}?useTimezone=true&serverTimezone=UTC&useSSL=false",
    conf.mySqlUser, conf.mySqlPassword)
  private val statement: Statement = con.createStatement()
  private val codec: Codec = conf.sqlEncoding.toLowerCase match
    case "iso8859-1" => scala.io.Codec.ISO8859
    case _           => scala.io.Codec.UTF8
  private val codAction: CodingErrorAction = CodingErrorAction.REPLACE
  private val decoder: CharsetDecoder = codec.decoder.onMalformedInput(codAction)
  private val reader: BufferedSource = Source.fromFile(conf.sqlf)(using Codec(decoder))
  private val content: String = reader.getLines().mkString(" ")

  print("Executing query ... ")
  private val rs: ResultSet = statement.executeQuery(content)
  println("OK")

  reader.close()

  /**
   * Returns the produced documents.
   * @return lazy list of produced documents
   */
  override def getDocuments: LazyList[Document] = getDocuments(Seq())

  /**
   * Returns the produced documents.
   *
   * @param previous documents prepared in earlier recursive calls
   * @return lazy list of produced documents
   */
  def getDocuments(previous: Seq[Document]): LazyList[Document] =
    previous match
      case head +: tail => head #:: getDocuments(tail)
      case _ =>
        if rs.next() then
          parseRecord(rs, conf.jsonFields, conf.repetitiveFields, conf.repetitiveSep) match
            case Success(doc +: tail) => doc #:: getDocuments(tail)
            case Success(_) => getDocuments(Seq.empty)
            case Failure(exception) =>
              Console.err.println(s"MysqlProducer/getDocuments/${exception.getMessage}")
              con.close()
              LazyList.empty
        else
          con.close()
          LazyList.empty

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
                          repetitiveSep: Option[String]): Try[Seq[Document]] =
    for
      fieldMap <- fieldNames(rs).flatMap:
        _.foldLeft(Try(Map.empty[String, Seq[String]])):
          case (acc, (column, fieldName)) =>
            for
              current <- acc
              values <- extractFieldValues(rs, column, fieldName, jsonFields, repetitiveFields, repetitiveSep)
            yield current ++ values
    yield genDocSeq(fieldMap)

  /**
   * Resolves the database column names for the current result set.
   *
   * @param rs result set positioned at the current database row
   * @return result containing the indexed column names
   */
  private def fieldNames(rs: ResultSet): Try[Seq[(Int, String)]] =
    Try:
      val meta: ResultSetMetaData = rs.getMetaData
      (1 to meta.getColumnCount).map(column => column -> meta.getColumnName(column))

  /**
   * Extracts the values associated with the current database column.
   *
   * @param rs result set positioned at the current database row
   * @param column database column index
   * @param fieldName logical field name associated with the column
   * @param jsonFields mapping used to expand JSON fields
   * @param repetitiveFields field names configured as repetitive
   * @param repetitiveSep separator used to split repetitive fields
   * @return result containing the normalized field map for the column
   */
  private def extractFieldValues(rs: ResultSet,
                                 column: Int,
                                 fieldName: String,
                                 jsonFields: Option[Map[String, Map[String, String]]],
                                 repetitiveFields: Option[Set[String]],
                                 repetitiveSep: Option[String]): Try[Map[String, Seq[String]]] =
    Option(rs.getString(column)).map(_.trim)
      .map:
        content =>
          jsonFields.flatMap(_.get(fieldName))
            .map(getJsonSeq(content, fieldName, _))
            .getOrElse(Success(Map(fieldName -> splitRepetitive(fieldName, content, repetitiveFields, repetitiveSep))))
      .getOrElse(Success(Map(fieldName -> Seq.empty)))

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
   * Extracts the configured JSON values from the current field.
   *
   * @param jsonStr raw JSON content to parse
   * @param jsonFieldName name assigned to the extracted JSON field
   * @param jsonFields mapping that describes how JSON fields should be extracted
   * @return mapped JSON values extracted from the field
   */
  private def getJsonSeq(jsonStr: String,
                         jsonFieldName: String,
                         jsonFields: Map[String, String]): Try[Map[String, Seq[String]]]=
    Try:
      require(jsonStr.trim.nonEmpty)

      jsonStr.trim match
        case "" => Map(jsonFieldName -> Seq(""))
        case jstr =>
          Json.parse(jstr) match
            case arr: JsArray =>
              val seq: Seq[JsValue] = arr.value.toSeq
              seq.headOption match
                case Some(obj: JsObject) =>
                  extractJsonObjectFields(obj, jsonFields)
                case Some(_) => Map(jsonFieldName -> seq.map(_.toString()))
                case None => Map(jsonFieldName -> Seq(""))
            case obj: JsObject =>
              extractJsonObjectFields(obj, jsonFields)
            case str: JsString => Map(jsonFieldName -> Seq(str.toString()))
            case other => throw new IllegalArgumentException(other.toString())

  /**
   * Extracts configured JSON paths from the given JSON object.
   *
   * @param obj parsed JSON object used as the extraction source
   * @param jsonFields mapping between JSON paths and output field names
   * @return grouped field values extracted from the JSON object
   */
  private def extractJsonObjectFields(obj: JsObject,
                                      jsonFields: Map[String, String]): Map[String, Seq[String]] =
    jsonFields.toSeq.flatMap:
      case (path, newName) => (obj \ path).asOpt[String].map(newName -> _)
    .groupMap(_._1)(_._2)

  /**
   * Generates the document sequence represented by the field map.
   *
   * @param map field map used to generate documents
   * @return generated document sequence
   */
  private def genDocSeq(map: Map[String, Seq[String]]): Seq[Document] =
    map.toSeq.foldRight(Seq(Document(Seq.empty))):
      case ((field, values), documents) =>
        val currentValues = values match
          case Seq() => Seq("")
          case nonEmpty => nonEmpty
        for
          value <- currentValues
          document <- documents
        yield Document(document.fields.appended(field -> value))
