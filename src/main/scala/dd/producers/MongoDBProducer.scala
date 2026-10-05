package dd.producers

import com.mongodb.client.{FindIterable, MongoClient, MongoClients, MongoCollection, MongoCursor}
import dd.interfaces.{DocsProducer, Document}
import org.bson.Document as BsonDocument

import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

/**
 * Immutable configuration used by the MongoDB-backed document producer.
 *
 * Query and projection are optional JSON documents accepted by the MongoDB
 * driver. When fields is defined, each entry must use the
 * `outputField=mongo.path` format. The output order follows the configured
 * field order. noCursorTimeout defaults to true because downstream duplicate
 * checks may spend a long time processing each document before reading the next
 * one from MongoDB.
 *
 * @param database MongoDB database name
 * @param collection MongoDB collection name
 * @param query optional JSON query document
 * @param projection optional JSON projection document
 * @param host optional MongoDB host, defaulting to localhost
 * @param port optional MongoDB port, defaulting to 27017
 * @param user optional MongoDB username
 * @param password optional MongoDB password
 * @param fields optional output field mappings in `outputField=mongo.path` format
 * @param noCursorTimeout whether to disable server-side cursor timeout
 */
case class MongoDBProducerConfig(database: String,
                                 collection: String,
                                 query: Option[String] = None,
                                 projection: Option[String] = None,
                                 host: Option[String] = None,
                                 port: Option[Int] = None,
                                 user: Option[String] = None,
                                 password: Option[String] = None,
                                 fields: Option[Seq[String]] = None,
                                 noCursorTimeout: Boolean = true):
  require(database.trim.nonEmpty)
  require(collection.trim.nonEmpty)
  require(port.forall(_ > 0))
  require(fields.forall(_.forall(field => MongoDBProducer.parseFieldMapping(field).isSuccess)))

/**
 * MongoDB-backed producer that streams documents from a collection.
 *
 * The producer applies an optional query/projection pair and converts each BSON
 * document into the internal document representation. Arrays are serialized as
 * a single field value joined by the default field separator; nested documents
 * are serialized as compact JSON when selected as a final path value.
 *
 * @param conf MongoDB producer configuration
 */
class MongoDBProducer(conf: MongoDBProducerConfig) extends DocsProducer:
  private val mongoClient: MongoClient = MongoClients.create(MongoDBProducer.mongoUri(conf))
  private val fieldMappings: Option[Seq[MongoDBProducer.FieldMapping]] =
    conf.fields.map(_.map(MongoDBProducer.parseFieldMapping(_).get))

  /**
   * Returns the produced documents.
   * @return lazy list of produced documents
   */
  override def getDocuments: LazyList[Document] =
    Try:
      val collection: MongoCollection[BsonDocument] =
        mongoClient.getDatabase(conf.database).getCollection(conf.collection)
      val query: BsonDocument = conf.query.map(BsonDocument.parse).getOrElse(new BsonDocument())
      val finder: FindIterable[BsonDocument] = collection.find(query)
      val projected: FindIterable[BsonDocument] =
        conf.projection.map(projection => finder.projection(BsonDocument.parse(projection))).getOrElse(finder)
      projected.noCursorTimeout(conf.noCursorTimeout)

      getDocumentsLazy(projected.iterator())
    match
      case Success(list) => list
      case Failure(exception) =>
        Console.err.println(s"MongoDBProducer/getDocuments/${exception.getMessage}")
        closeClient()
        LazyList.empty

  /**
   * Streams MongoDB cursor entries into the internal lazy document sequence.
   *
   * Each cursor element is converted on demand, so downstream processing can
   * consume one document at a time without materializing the whole collection.
   * Conversion failures are logged and skipped; cursor failures close the
   * cursor and client before returning an empty tail.
   *
   * @param cursor MongoDB cursor currently being consumed
   * @return lazy list containing converted documents from the cursor
   */
  private def getDocumentsLazy(cursor: MongoCursor[BsonDocument]): LazyList[Document] =
    Try(cursor.hasNext) match
      case Success(true) =>
        Try(cursor.next()).flatMap(MongoDBProducer.toDocument(_, fieldMappings)) match
          case Success(document) => document #:: getDocumentsLazy(cursor)
          case Failure(exception) =>
            Console.err.println(s"MongoDBProducer/getDocumentsLazy/${exception.getMessage}")
            getDocumentsLazy(cursor)
      case Success(false) =>
        close(cursor)
        LazyList.empty
      case Failure(exception) =>
        Console.err.println(s"MongoDBProducer/getDocumentsLazy/${exception.getMessage}")
        close(cursor)
        LazyList.empty

  /**
   * Closes the MongoDB cursor and its owning client.
   *
   * @param cursor MongoDB cursor to close
   * @return no value; this method performs best-effort resource cleanup
   */
  private def close(cursor: MongoCursor[BsonDocument]): Unit =
    Try(cursor.close())
    closeClient()

  /**
   * Closes the MongoDB client associated with this producer.
   *
   * @return no value; this method performs best-effort client cleanup
   */
  private def closeClient(): Unit =
    Try(mongoClient.close())

/**
 * Helper functions used by MongoDBProducer and its tests.
 *
 * The companion object centralizes URI construction, configured field parsing,
 * path resolution, and BSON-to-Document conversion so those behaviors can be
 * validated without opening a live MongoDB connection.
 */
private[producers] object MongoDBProducer:
  val DefaultFieldSeparator = "¦"

  /**
   * Mapping between an output document field and a MongoDB path.
   *
   * @param fieldName field name emitted in the internal Document
   * @param path MongoDB field path split into path segments
   */
  case class FieldMapping(fieldName: String,
                          path: Seq[String])

  /**
   * Builds the MongoDB connection URI from the producer configuration.
   *
   * @param conf MongoDB producer configuration
   * @return MongoDB URI accepted by the Java driver
   */
  def mongoUri(conf: MongoDBProducerConfig): String =
    val usrPswStr = conf.user.flatMap:
      usr => conf.password.map(psw => s"$usr:$psw@")
    .getOrElse("")
    val mongoHost = conf.host.getOrElse("localhost")
    val mongoAddress =
      if mongoHost.contains(":") && conf.port.isEmpty then mongoHost
      else s"$mongoHost:${conf.port.getOrElse(27017)}"

    s"mongodb://$usrPswStr$mongoAddress"

  /**
   * Converts a BSON document into the internal document representation.
   *
   * When field mappings are provided, only mapped values are emitted and the
   * configured order is preserved. Without mappings, all top-level BSON fields
   * are emitted using their original names.
   *
   * @param document BSON document returned by MongoDB
   * @param fields optional configured field mappings
   * @return converted internal document, or a failure when conversion fails
   */
  def toDocument(document: BsonDocument,
                 fields: Option[Seq[FieldMapping]]): Try[Document] =
    Try:
      val selectedFields = fields.getOrElse:
        document.keySet().asScala.toSeq.map(field => FieldMapping(field, Seq(field)))

      val outputFields = selectedFields.map:
        mapping =>
          mapping.fieldName -> resolvePath(document, mapping.path).map(valuesToField).getOrElse("")

      Document(outputFields)

  /**
   * Parses one configured MongoDB field mapping.
   *
   * The accepted syntax is `outputField=mongo.path`, where the right-hand side
   * can contain one or more dot-separated MongoDB path segments.
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
          if normalizedPath.isEmpty then throw IllegalArgumentException(s"Empty MongoDB field path in mapping: $raw")

          FieldMapping(normalizedFieldName, normalizedPath)
        case _ =>
          throw IllegalArgumentException(s"Invalid MongoDB field mapping: $raw")

  /**
   * Resolves a dot-separated path against a BSON value.
   *
   * Arrays and iterables are traversed by applying the same remaining path to
   * every element, allowing mappings such as `author=authors.name` to collect a
   * subfield from every object in an array.
   *
   * @param value current value being inspected
   * @param path remaining MongoDB path segments to resolve
   * @return resolved values when the path exists, otherwise none
   */
  private def resolvePath(value: Any,
                          path: Seq[String]): Option[Seq[Any]] =
    path match
      case Seq() => Some(Seq(value))
      case segment +: tail =>
        value match
          case null => None
          case document: BsonDocument => resolvePath(document.get(segment), tail)
          case map: java.util.Map[?, ?] => resolvePath(map.get(segment), tail)
          case iterable: java.lang.Iterable[?] =>
            val values = iterable.asScala.toSeq.flatMap(item => resolvePath(item, path).getOrElse(Seq.empty))
            Some(values)
          case array: Array[?] =>
            val values = array.toSeq.flatMap(item => resolvePath(item, path).getOrElse(Seq.empty))
            Some(values)
          case _ => None

  /**
   * Serializes one or more resolved values into a single document field value.
   *
   * Multiple values are joined with the default field separator so arrays do not
   * expand into repeated internal fields.
   *
   * @param values resolved MongoDB values
   * @return serialized field value
   */
  private def valuesToField(values: Seq[Any]): String =
    val strings = values.flatMap(valueToStrings)
    strings.mkString(DefaultFieldSeparator)

  /**
   * Converts an arbitrary MongoDB/BSON value into string values.
   *
   * Nested BSON documents are preserved as compact JSON. Arrays and iterables
   * are flattened recursively so the caller can join them with the configured
   * field separator.
   *
   * @param value MongoDB/BSON value to serialize
   * @return string representation values derived from the input
   */
  private def valueToStrings(value: Any): Seq[String] =
    value match
      case null => Seq("")
      case nested: BsonDocument => Seq(nested.toJson)
      case iterable: java.lang.Iterable[?] =>
        iterable.asScala.toSeq.flatMap(valueToStrings)
      case array: Array[?] =>
        array.toSeq.flatMap(valueToStrings)
      case other => Seq(other.toString)
