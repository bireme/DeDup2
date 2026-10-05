package dd.producers

import dd.NGAnalyzer
import dd.interfaces.{DocsProducer, Document}
import dd.tools.Tools
import org.apache.lucene.index.{DirectoryReader, FieldInfos}
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser
import org.apache.lucene.search.{IndexSearcher, MatchAllDocsQuery, Query, ScoreDoc}
import org.apache.lucene.store.FSDirectory

import java.nio.file.Paths
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

/**
 * Produces internal documents stored in a Lucene index.
 *
 * When `search` is defined, its Lucene query syntax is used to select the
 * documents returned by the producer. When `fields` is defined, only stored
 * fields whose names occur in that sequence are included in each document.
 * The query is executed eagerly, while document conversion remains lazy.
 * Lucene resources are closed after the returned list is exhausted.
 *
 * @param indexPath path to the Lucene index
 * @param search optional Lucene query applied to indexed fields
 * @param fields optional names of stored fields returned in each document
 */
class LuceneProducer(indexPath: String,
                     search: Option[String] = None,
                     fields: Option[Seq[String]] = None) extends DocsProducer:
  private val directory: FSDirectory = FSDirectory.open(Paths.get(indexPath))
  private val reader: DirectoryReader = DirectoryReader.open(directory)
  private val searcher: IndexSearcher = new IndexSearcher(reader)
  private val analyzer: NGAnalyzer = new NGAnalyzer()
  private val selectedFields: Option[Set[String]] = fields.map(_.toSet)

  /**
   * Returns matching stored documents as a lazy list.
   *
   * @return documents selected by `search`, optionally restricted to `fields`
   */
  override def getDocuments: LazyList[Document] =
    Try:
      val query: Query = buildQuery()
      val limit: Int = math.max(1, reader.maxDoc())
      searcher.search(query, limit).scoreDocs
    match
      case Success(scoreDocs) => getDocumentsLazy(scoreDocs, 0)
      case Failure(exception) =>
        closeResources()
        Console.err.println(s"LuceneProducer/getDocuments/${exception.getMessage}")
        LazyList.empty[Document]

  /** Builds the configured query or a query that matches every index document. */
  private def buildQuery(): Query =
    search.map(_.trim).filter(_.nonEmpty) match
      case None => MatchAllDocsQuery.INSTANCE
      case Some(queryText) =>
        val indexedFields: Array[String] = FieldInfos.getIndexedFields(reader).asScala.toArray
        if indexedFields.isEmpty then MatchAllDocsQuery.INSTANCE
        else new MultiFieldQueryParser(indexedFields, analyzer).parse(queryText)

  /**
   * Converts one result page entry and continues until all hits are consumed.
   *
   * @param scoreDocs matching Lucene score documents
   * @param position current score document position
   * @return converted documents from the remaining hits
   */
  private def getDocumentsLazy(scoreDocs: Array[ScoreDoc], position: Int): LazyList[Document] =
    if position >= scoreDocs.length then
      closeResources()
      LazyList.empty[Document]
    else
      Try(searcher.storedFields().document(scoreDocs(position).doc)).toOption match
        case Some(luceneDocument) =>
          val document: Document = filterFields(Tools.doc2doc(luceneDocument))
          document #:: getDocumentsLazy(scoreDocs, position + 1)
        case None => getDocumentsLazy(scoreDocs, position + 1)

  /**
   * Restricts a converted document to the configured stored field names.
   *
   * @param document converted Lucene document
   * @return document containing only selected fields
   */
  private def filterFields(document: Document): Document =
    selectedFields match
      case None => document
      case Some(names) => document.copy(fields = document.fields.filter(field => names.contains(field._1)))

  /** Closes all resources owned by this producer. */
  private def closeResources(): Unit =
    analyzer.close()
    reader.close()
    directory.close()
