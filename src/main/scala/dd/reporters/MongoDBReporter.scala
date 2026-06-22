package dd.reporters

import com.mongodb.client.model.InsertManyOptions
import com.mongodb.client.{MongoClient, MongoClients, MongoCollection, MongoDatabase}
import dd.interfaces.{CompResult, Document, Reporter}
import org.bson

import scala.collection.mutable
import scala.jdk.CollectionConverters.BufferHasAsJava
import scala.util.{Failure, Success, Try}

/**
 * Reporter that stores comparison results in a MongoDB collection.
 *
 * The reporter converts each document comparison into a BSON document, buffers
 * the generated records in memory, and flushes them in batches to MongoDB for
 * more efficient persistence.
 */
class MongoDBReporter(database: String,
                      collection: String,
                      append: Boolean,
                      host: Option[String] = None,
                      port: Option[Int] = None,
                      user: Option[String] = None,
                      password: Option[String] = None,
                      minTrue: Int = 0,
                      flushResults: Boolean = false) extends Reporter:
  private val usrPswStr: String = user.flatMap:
    usr => password.map(psw => s"$usr:$psw@")
  .getOrElse("")
  private val mongoHost: String = host.getOrElse("localhost")
  private val mongoAddress: String =
    if mongoHost.contains(":") && port.isEmpty then mongoHost
    else s"$mongoHost:${port.getOrElse(27017)}"
  private val mongoUri: String = s"mongodb://$usrPswStr$mongoAddress"
  private val mongoClient: MongoClient = MongoClients.create(mongoUri)
  private val dbase: MongoDatabase = mongoClient.getDatabase(database)
  private val coll: MongoCollection[bson.Document] =
    if append then dbase.getCollection(collection)
    else
      val coll1: MongoCollection[bson.Document] = dbase.getCollection(collection)
      coll1.drop()
      dbase.getCollection(collection)
  private val buffer: mutable.Buffer[bson.Document] = mutable.Buffer[bson.Document]()

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
    if MongoDBReporter.shouldWriteResults(results, minTrue) then
      val document = buildReportDocument(originalDoc, currentDoc, otherFields, results)
      insertDoc(document, buffer, coll)
    else Success(())

  /**
   * Closes the underlying resources.
   * @return result of closing the underlying resources
   */
  override def close(): Try[Unit] =
    val flushed = flushBuffer(buffer, coll)
    val closed = Try(mongoClient.close())

    flushed match
      case Success(_) => closed
      case Failure(exception) =>
        closed.recover:
          case closeException => exception.addSuppressed(closeException)
        Failure(exception)

  /**
   * Adds the document to the current MongoDB batch.
   *
   * @param doc document to insert or serialize
   * @param buffer buffer that stores documents before flushing them
   * @param coll MongoDB collection that receives the buffered documents
   * @param maxSize value of max size
   * @return result of inserting the document into the buffer
   */
  private def insertDoc(doc: bson.Document,
                        buffer: mutable.Buffer[bson.Document],
                        coll: MongoCollection[bson.Document],
                        maxSize: Int = 100): Try[Unit] =
    Try:
      buffer.addOne(doc)
    .flatMap:
      _ => Option.when(flushResults || buffer.size >= maxSize)(flushBuffer(buffer, coll)).getOrElse(Success(()))

  /**
   * Flushes the buffered MongoDB documents.
   *
   * @param buffer buffer that stores documents before flushing them
   * @param coll MongoDB collection that receives the buffered documents
   * @return result of flushing the buffered documents
   */
  private def flushBuffer(buffer: mutable.Buffer[bson.Document],
                          coll: MongoCollection[bson.Document]): Try[Unit] =
    Try:
      if buffer.nonEmpty then
        coll.insertMany(buffer.asJava, new InsertManyOptions().ordered(false))
        buffer.clear()

  /**
   * Builds the BSON document persisted for a comparison result set.
   *
   * @param originalDoc source document used in the comparison
   * @param currentDoc candidate document being evaluated
   * @param otherFields additional field names included in the output
   * @param results comparison results produced for the document pair
   * @return BSON document representing the comparison output
   */
  private def buildReportDocument(originalDoc: Document,
                                  currentDoc: Document,
                                  otherFields: Seq[String],
                                  results: Seq[CompResult]): bson.Document =
    val baseFields = otherFields.flatMap:
      field =>
        Seq(
          s"${field}_1" -> getFieldValue(originalDoc, field),
          s"${field}_2" -> getFieldValue(currentDoc, field)
        )

    val resultFields = results.map(MongoDBReporter.resultFieldFor)
    (baseFields ++ resultFields).foldLeft(new bson.Document()):
      case (doc, (key, value)) => doc.append(key, value)

  /**
   * Returns the first value associated with a field in the provided document.
   *
   * @param document document that provides the field value
   * @param fieldName field name associated with the operation
   * @return first matching value when present, otherwise an empty string
   */
  private def getFieldValue(document: Document,
                            fieldName: String): String =
    document.fields.collectFirst:
      case (`fieldName`, value) => value
    .getOrElse("")

private[reporters] object MongoDBReporter:
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
   * Converts a single comparison result into the nested BSON format persisted by
   * the MongoDB reporter.
   *
   * @param result comparison result currently being serialized
   * @return BSON field entry derived from the result field name
   */
  def resultFieldFor(result: CompResult): (String, bson.Document) =
    val fieldKey =
      if result.originalField.isEmpty || result.currentField.isEmpty then s"${result.fieldName}_*"
      else result.fieldName

    fieldKey -> new bson.Document()
      .append("name", result.name)
      .append("originalField", result.originalField)
      .append("currentField", result.currentField)
      .append("originalFieldOther", result.originalFieldOther.getOrElse(""))
      .append("currentFieldOther", result.currentFieldOther.getOrElse(""))
      .append("similarity", result.similarity)
      .append("isSimilar", ReporterSimilarityStatus.displayValue(result))
