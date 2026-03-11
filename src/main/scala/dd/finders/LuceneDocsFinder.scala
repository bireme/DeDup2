package dd.finders

import dd.NGAnalyzer
import dd.interfaces.{DocsFinder, DocsProducer, Document}
import dd.tools.Tools
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{IndexSearcher, Query, ScoreDoc}
import org.apache.lucene.store.{Directory, FSDirectory}

import java.io.File
import java.nio.file.Path
import scala.util.Try

/**
 * Finder implementation backed by a Lucene index.
 *
 * The finder parses the incoming query against the configured search field,
 * executes the Lucene search, and exposes the matched stored documents through
 * a lazy producer consumed by the rest of the deduplication pipeline.
 */
class LuceneDocsFinder(luceneIndex: String,
                       searchField: String) extends DocsFinder:
  private val indexPath: Path = new File(luceneIndex).toPath
  private val directory: Directory = FSDirectory.open(indexPath)
  private val ireader: DirectoryReader = DirectoryReader.open(directory)
  private val isearcher: IndexSearcher = new IndexSearcher(ireader)
  private val analyzer: NGAnalyzer = new NGAnalyzer(search = true)

  /**
   * Finds the documents that match the given query.
   *
   * @param query main query string used to search for documents
   * @param auxQuery optional secondary query used to refine the search
   * @param maxDocs maximum number of documents to retrieve
   * @return result containing the produced documents
   */
  def findDocs(query: String,
               auxQuery: Option[String],
               maxDocs: Int = 100): Try[DocsProducer] =
    Try:
      require(query != null)

      val parser: QueryParser = new QueryParser("", analyzer)
      val qur: Query = parser.parse(auxQuery match
        case Some(aqry) => s"$searchField:($query) AND $aqry"
        case None => s"$searchField:($query)"
      )

      val hits: Array[ScoreDoc] = isearcher.search(qur, maxDocs).scoreDocs
      val ids: List[Int] = hits.map(sd => sd.doc).toList

      new DocsProducer:
        /**
         * Returns the produced documents.
         * @return lazy list of produced documents
         */
        def getDocuments: LazyList[Document] = lazyList(ids)

  /**
   * Returns the configured search field, if any.
   * @return configured search field when available
   */
  def getSearchField: Option[String] = Option(searchField)

  /**
   * Closes the underlying resources.
   * @return result of closing the underlying resources
   */
  def close(): Try[Unit] =
    Try:
      ireader.close()
      directory.close()

  /**
   * Builds a lazy list from the available document identifiers.
   *
   * @param list document identifiers to expose as a lazy list
   * @return lazy list of converted documents
   */
  private def lazyList(list: List[Int]): LazyList[Document] = list match
    case h :: t => Tools.doc2doc(ireader.storedFields().document(h)) #:: lazyList(t)
    case Nil => LazyList[Document]()
