package dd.tools

import dd.interfaces.{DocsProducer, Document}
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.document
import org.apache.lucene.document.{Field, StoredField, TextField}
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.{Directory, FSDirectory}
import play.api.libs.json.{JsArray, JsObject, JsString, JsValue, Json}

import java.io.File
import java.nio.file.Path
import java.text.Normalizer
import java.text.Normalizer.Form
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.{Failure, Success, Try}

/**
 * Shared utility functions used across indexing, conversion, and serialization.
 *
 * This module centralizes reusable helpers for text normalization, Lucene
 * document conversion, index creation, and JSON serialization so the rest of
 * the codebase can stay focused on domain-specific processing.
 */
object Tools:
  /**
   * Normalizes a string for comparison.
   *
   * @param in input string to normalize
   * @return normalized string value
   */
  def normalizeStr(in: String): String = Normalizer.normalize(in.trim().toLowerCase, Form.NFD)
    .replaceAll("[^a-z0-9]", "")

  /**
   * Creates a Lucene index from the produced documents.
   *
   * @param producer document producer used as the indexing source
   * @param indexPath target path of the Lucene index
   * @param fieldToIndex document field indexed for similarity searches
   * @param analyzer value of analyzer
   * @return result of creating the Lucene index
   */
  def createLuceneIndex(producer: DocsProducer,
                        indexPath: String,
                        fieldToIndex: String,
                        analyzer: Analyzer): Try[Unit] =
    Try:
      val iPath: Path = new File(indexPath).toPath
      val directory: Directory = FSDirectory.open(iPath)
      val config: IndexWriterConfig = new IndexWriterConfig(analyzer).setOpenMode(IndexWriterConfig.OpenMode.CREATE)
      val iwriter: IndexWriter = new IndexWriter(directory, config)

      producer.getDocuments.zipWithIndex.foreach:
        case (dc, idx) =>
          if idx % 100_000 == 0 then println(s"+++$idx")

          val lucDoc: document.Document = dc.fields.foldLeft(new org.apache.lucene.document.Document()):
            case (ldoc, (fldName, fldValue)) =>
              if fldName.equals(fieldToIndex) then
                if fldValue.trim.isEmpty then Console.err.println("Error indexing document field is empty")
                else ldoc.add(new TextField(fldName, fldValue, Field.Store.YES))
              else ldoc.add(new StoredField(fldName, fldValue))
              ldoc

          iwriter.addDocument(lucDoc)

      iwriter.forceMerge(1)
      iwriter.close()
      directory.close()

  /**
   * Converts a Lucene document into the internal document model.
   *
   * @param ldoc value of ldoc
   * @return internal document created from the Lucene document
   */
  def doc2doc(ldoc: org.apache.lucene.document.Document): Document =
    val fields: Seq[(String, String)] = ldoc.iterator.asScala.map:
      field => field.name() -> field.stringValue()
    .toSeq
    Document(fields)

  /**
   * Converts an internal document into JSON.
   *
   * @param doc document to insert or serialize
   * @param allFldsAreArray whether every JSON field should be emitted as an array
   * @return JSON representation of the internal document
   */
  def doc2json(doc: Document,
               allFldsAreArray: Boolean = false): JsValue =
    val map1: Map[String, Seq[JsValue]] = doc.fields.foldLeft(Map[String, Seq[JsValue]]()):
      case (mp, (k,v)) =>
        val vT: String = v.trim
        val field: JsValue =
          if vT.startsWith("[") || vT.startsWith("{") then
            Try(Json.parse(vT)) match
              case Success(json) => json
              case Failure(_) => JsString(vT)
          else JsString(vT)
        mp + (k -> mp.getOrElse(k, Seq[JsValue]()).appended(field))

    val map2: Map[String, JsArray] = map1.map:
      case (k, seq) => k -> JsArray(seq)

    val seq1: Seq[(String, JsArray)] = map2.toSeq
    val seq2: Seq[(String, JsValue)] =
      if allFldsAreArray then seq1
      else
        seq1.map:
          case (k, seq) =>
            if seq.value.length == 1 then k -> seq.value.head
            else k -> seq

    JsObject(seq2)
