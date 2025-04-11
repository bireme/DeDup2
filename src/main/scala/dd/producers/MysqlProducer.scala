package dd.producers

import dd.interfaces.{DocsProducer, Document}

import java.nio.charset.{Charset, CharsetDecoder, CodingErrorAction}
import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}
import play.api.libs.json.{JsArray, JsObject, JsString, JsValue, Json}

import scala.io.{BufferedSource, Codec, Source}
import scala.util.{Failure, Success, Try}

case class MySqlProducerConfig(mySqlHost: String,
                               mySqlPort: Int,
                               mySqlDbname: String,
                               mySqlUser: String,
                               mySqlPassword: String,
                               sqlf: String,                    // file having the sql statement
                               sqlEncoding: String,             // the sql file character encoding
                               jsonFields: Option[Map[String, Map[String, String]]],  // map a sql table field name (with json content) with a collection of json field path to extract and it's new field name.
                               repetitiveFields: Option[Set[String]],              // the name of the fields that should be broken into a new line when the repetitive the separator symbol is defined.
                               repetitiveSep: Option[String]) { // string used to split repetitive fields
  require(mySqlHost.trim.nonEmpty)
  require(mySqlPort > 0)
  require(mySqlDbname.trim.nonEmpty)
  require(sqlf.trim.nonEmpty)
  require(sqlEncoding.trim.nonEmpty)
  require(repetitiveFields.isEmpty || repetitiveSep.isDefined)
}
/**
 * A producer of documents coming from a MySQL database
 * @param conf class with the parameters used by this class
 */
class MysqlProducer(conf: MySqlProducerConfig) extends DocsProducer {
  private val con: Connection = DriverManager.getConnection(
    //s"jdbc:mysql://${host.trim}:3306/${dbnm.trim}",
    s"jdbc:mysql://${conf.mySqlHost.trim}:${conf.mySqlPort}/${conf.mySqlDbname.trim}?useTimezone=true&serverTimezone=UTC&useSSL=false",
    conf.mySqlUser, conf.mySqlPassword)
  private val statement: Statement = con.createStatement()
  private val codec: Codec = conf.sqlEncoding.toLowerCase match {
    case "iso8859-1" => scala.io.Codec.ISO8859
    case _           => scala.io.Codec.UTF8
  }
  private val codAction: CodingErrorAction = CodingErrorAction.REPLACE
  private val decoder: CharsetDecoder = codec.decoder.onMalformedInput(codAction)
  private val reader: BufferedSource = Source.fromFile(conf.sqlf)(decoder)
  private val content: String = reader.getLines().mkString(" ")

  print("Executing query ... ")
  private val rs: ResultSet = statement.executeQuery(content)
  println("OK")

  reader.close()

  override def getDocuments: LazyList[Document] = getDocuments(Seq())

  def getDocuments(previous: Seq[Document]): LazyList[Document] = {
    previous.headOption match {
      case Some(prev) => prev #:: getDocuments(previous.tail)
      case None =>
        if (rs.next()) {
          parseRecord(rs, conf.jsonFields, conf.repetitiveFields, conf.repetitiveSep) match {
            case Success(docs) =>
              docs.headOption match {
                case Some(doc) => doc #:: getDocuments(docs.tail)
                case None => getDocuments(Seq())
              }
            case Failure(exception) =>
              exception.printStackTrace()
              con.close()
              LazyList.empty
          }
        } else {
          con.close()
          LazyList.empty
        }
    }
  }

  /** Given a record retrieved by a query, returns a list of the contents of the
   * desired fields.
   *
   * @param rs result set object whose current position points to the retrieved record
   * @param jsonFields name of the sql column -> (json path -> new field name)
   * @param repetitiveSep the string used to split a field content into more than one occurrence
   *
   * @return a sequence of Document objects
   */
  private def parseRecord(rs: ResultSet,
                          jsonFields: Option[Map[String, Map[String, String]]],
                          repetitiveFields: Option[Set[String]],
                          repetitiveSep: Option[String]): Try[Seq[Document]] = {
    Try {
      val meta: ResultSetMetaData = rs.getMetaData
      val cols: Int = meta.getColumnCount
      val names: Map[Int, String] = (1 to cols).foldLeft(Map[Int, String]()) {
        case (map, col) => map + (col -> meta.getColumnName(col))
      }
      val auxMap: Map[String, Seq[String]] = (1 to cols).foldLeft[Map[String, Seq[String]]](Map()) {
        case (map, col) =>
          val fName: String = names(col)

          Option(rs.getString(col)) match {
            case Some(fContent) =>
              val fContentT: String = fContent.trim

              jsonFields match {
                case Some(jFields) => jFields.get(fName) match {
                  case Some(map1: Map[String, String]) => getJsonSeq(fContentT, fName, map1) match {
                    case Success(map2) => map ++ map2
                    case Failure(exception) => throw exception
                  }
                  case None => repetitiveFields match {
                    case Some(rFields) if rFields.nonEmpty && rFields.contains(fName) => map + (fName -> fContentT.split(repetitiveSep.get).toSeq)
                    case _ => map + (fName -> Seq(fContentT))
                  }
                }
                case None => repetitiveFields match {
                  case Some(rFields) if rFields.nonEmpty && rFields.contains(fName) => map + (fName -> fContentT.split(repetitiveSep.get).toSeq)
                  case _ => map + (fName -> Seq(fContentT))
                }
              }
            case None => map + (fName -> Seq[String]())
          }
      }
      auxMap
    }.map(map => genDocSeq(map))
  }

  private def getJsonSeq(jsonStr: String,
                         jsonFieldName: String,
                         jsonFields: Map[String, String]): Try[Map[String, Seq[String]]] = {
    Try {
      require(jsonStr.trim.nonEmpty)

      jsonStr.trim match {
        case "" => Map(jsonFieldName -> Seq(""))
        case jstr =>
          Json.parse(jstr) match {
            case arr: JsArray =>
              val seq: Seq[JsValue] = arr.value.toSeq
              seq.headOption match {
                case Some(obj: JsObject) =>
                  jsonFields.foldLeft(Map[String, Seq[String]]()) {
                    case (map, (path, newName)) =>
                      (obj \ path).asOpt[String] match {
                        case Some(fld) =>
                          val fields: Seq[String] = map.getOrElse(newName, Seq[String]()).appended(fld)
                          map + (newName -> fields)
                        case None => map
                      }
                  }
                case Some(_) => Map(jsonFieldName -> seq.map(_.toString()))
                case None => Map(jsonFieldName -> Seq(""))
              }
            case obj: JsObject =>
              jsonFields.foldLeft(Map[String, Seq[String]]()) {
                case (map, (path, newName)) =>
                  (obj \ path).asOpt[String] match {
                    case Some(fld) =>
                      val fields: Seq[String] = map.getOrElse(newName, Seq[String]()).appended(fld)
                      map + (newName -> fields)
                    case None => map
                  }
              }
            case str: JsString => Map(jsonFieldName -> Seq(str.toString()))
            case other => throw new IllegalArgumentException(other.toString())
          }
      }
    }
  }

  private def genDocSeq(map: Map[String, Seq[String]]): Seq[Document] = {
    map.headOption match {
      case Some((key, seq)) =>
        val previous: Seq[Document] = genDocSeq(map.tail)

        if (previous.isEmpty) seq.map(content => Document(Seq(key -> content)))
        else seq.flatMap(content => previous.map(doc => Document(doc.fields.appended(key -> content))))
      case None => Seq[Document]()
    }
  }

  private def isUtf8Encoding(text: String): Boolean = {
    require(text != null)

    val utf8: Charset = Charset.availableCharsets().get("UTF-8")
    val b1: Array[Byte] = text.getBytes(utf8)
    val b2: Array[Byte] = new String(b1, utf8).getBytes(utf8)

    b1.sameElements(b2)
  }
}