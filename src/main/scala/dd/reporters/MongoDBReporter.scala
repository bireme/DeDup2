package dd.reporters

import com.mongodb.client.model.InsertManyOptions
import com.mongodb.client.result.InsertManyResult
import com.mongodb.client.{MongoClient, MongoClients, MongoCollection, MongoDatabase}
import dd.interfaces.{CompResult, Document, Reporter}
import org.bson

import scala.collection.mutable
import scala.jdk.CollectionConverters.BufferHasAsJava
import scala.util.Try

class MongoDBReporter(database: String,
                      collection: String,
                      append: Boolean,
                      host: Option[String] = None,
                      port: Option[Int] = None,
                      user: Option[String] = None,
                      password: Option[String] = None) extends Reporter {
  private val usrPswStr: String = user.flatMap {
    usr => password.map(psw => s"$usr:$psw@")
  }.getOrElse("")
  private val mongoUri: String = s"mongodb://$usrPswStr${host.getOrElse("localhost")}:${port.getOrElse(27017)}"
  private val mongoClient: MongoClient = MongoClients.create(mongoUri)
  private val dbase: MongoDatabase = mongoClient.getDatabase(database)
  private val coll: MongoCollection[bson.Document] = if (append) dbase.getCollection(collection)
  else {
    val coll1: MongoCollection[bson.Document] = dbase.getCollection(collection)

    coll1.drop()
    dbase.getCollection(collection)
  }
  private val buffer: mutable.Buffer[bson.Document] = mutable.Buffer[bson.Document]()

  override def writeResults(originalDoc: Document,
                            currentDoc: Document,
                            otherFields: Seq[String],
                            results: Seq[CompResult]): Try[Unit] = {
    val document: bson.Document = results.foldLeft(new bson.Document()) {
      case (doc, result) =>
        val doc1: bson.Document = doc.append("comparator", result.name)
          .append(s"${result.fieldName}_1", originalDoc.fields.filter(fld => fld._1.equals(result.fieldName)))
          .append(s"${result.fieldName}_2", currentDoc.fields.filter(fld => fld._1.equals(result.fieldName)))
          .append(s"${result.fieldName}_other_1", result.originalFieldOther.getOrElse(""))
          .append(s"${result.fieldName}_other_2", result.currentFieldOther.getOrElse(""))
        val doc2: bson.Document = otherFields.foldLeft(new bson.Document()) {
          case (d, oField) =>
            d.append(s"${oField}_1", originalDoc.fields.find(fld => fld._1.equals(oField)).getOrElse(("", ""))._2)
            d.append(s"${oField}_2", currentDoc.fields.find(fld => fld._1.equals(oField)).getOrElse(("", ""))._2)
        }

        doc2.append(s"${result.fieldName}_2", currentDoc.fields.filter(fld => fld._1.equals(result.fieldName)))
          .append("similarity", result.similarity)
          .append("isSimilar", result.isSimilar)

        doc1.putAll(doc2)
        doc1
    }
    insertDoc(document, buffer, coll)
  }

  override def close(): Try[Unit] = Try {
    flushBuffer(buffer, coll)
    mongoClient.close()
  }

  private def insertDoc(doc: bson.Document,
                        buffer: mutable.Buffer[bson.Document],
                        coll: MongoCollection[bson.Document],
                        maxSize: Int = 10000): Try[Unit] = {
    Try {
      val docId: String = Option(doc.get("_metadata")).flatMap(meta => Option(meta.asInstanceOf[bson.Document].get("Id")))
        .map(_.toString).getOrElse("???")
      println(s">>> writing doc - id:$docId")
      buffer.addOne(doc)
      if (buffer.size >= maxSize) {
        val t: Try[InsertManyResult] = Try(coll.insertMany(buffer.asJava, new InsertManyOptions().ordered(false)))
        buffer.clear()
        t recover { case exception: Exception => throw exception }
      }
    }
  }

  private def flushBuffer(buffer: mutable.Buffer[bson.Document],
                          coll: MongoCollection[bson.Document]): Try[Unit] = {
    Try {
      if (buffer.nonEmpty) {
        val t: Try[InsertManyResult] = Try(coll.insertMany(buffer.asJava, new InsertManyOptions().ordered(false)))
        buffer.clear()
        t recover { case exception: Exception => throw exception }
      }
    }
  }
}
