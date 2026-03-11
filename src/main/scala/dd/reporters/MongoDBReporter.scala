package dd.reporters

import com.mongodb.client.model.InsertManyOptions
import com.mongodb.client.{MongoClient, MongoClients, MongoCollection, MongoDatabase}
import dd.interfaces.{CompResult, Document, Reporter}
import org.bson

import scala.collection.mutable
import scala.jdk.CollectionConverters.BufferHasAsJava
import scala.util.{Success, Try}

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
                      password: Option[String] = None) extends Reporter:
  private val usrPswStr: String = user.flatMap:
    usr => password.map(psw => s"$usr:$psw@")
  .getOrElse("")
  private val mongoUri: String = s"mongodb://$usrPswStr${host.getOrElse("localhost")}:${port.getOrElse(27017)}"
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
    val document = buildReportDocument(originalDoc, currentDoc, otherFields, results)
    insertDoc(document, buffer, coll)

  /**
   * Closes the underlying resources.
   * @return result of closing the underlying resources
   */
  override def close(): Try[Unit] = Try:
    flushBuffer(buffer, coll)
    mongoClient.close()

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
                        maxSize: Int = 10000): Try[Unit] =
    Try:
      val docId: String = Option(doc.get("_metadata")).flatMap(meta => Option(meta.asInstanceOf[bson.Document].get("Id")))
        .map(_.toString).getOrElse("???")
      println(s">>> writing doc - id:$docId")
      buffer.addOne(doc)
    .flatMap:
      _ => Option.when(buffer.size >= maxSize)(flushBuffer(buffer, coll)).getOrElse(Success(()))

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

    val resultFields = results.flatMap(resultFieldsFor(originalDoc, currentDoc, _))
    (baseFields ++ resultFields).foldLeft(new bson.Document()):
      case (doc, (key, value)) => doc.append(key, value)

  /**
   * Converts a single comparison result into BSON field entries.
   *
   * @param originalDoc source document used in the comparison
   * @param currentDoc candidate document being evaluated
   * @param result comparison result currently being serialized
   * @return sequence of BSON field entries derived from the result
   */
  private def resultFieldsFor(originalDoc: Document,
                              currentDoc: Document,
                              result: CompResult): Seq[(String, Any)] =
    Seq(
      "comparator" -> result.name,
      s"${result.fieldName}_1" -> getFieldValues(originalDoc, result.fieldName),
      s"${result.fieldName}_2" -> getFieldValues(currentDoc, result.fieldName),
      s"${result.fieldName}_other_1" -> result.originalFieldOther.getOrElse(""),
      s"${result.fieldName}_other_2" -> result.currentFieldOther.getOrElse(""),
      "similarity" -> result.similarity,
      "isSimilar" -> result.isSimilar
    )

  /**
   * Returns all values associated with a field in the provided document.
   *
   * @param document document that provides the field values
   * @param fieldName field name associated with the operation
   * @return sequence of field pairs that match the requested name
   */
  private def getFieldValues(document: Document,
                             fieldName: String): Seq[(String, String)] =
    document.fields.filter(_._1 == fieldName)

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
