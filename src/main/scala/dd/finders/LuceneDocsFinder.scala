package dd.finders

import dd.interfaces.{DocsFinder, DocsProducer, Document}
import dd.NGAnalyzer
import dd.tools.Tools
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{IndexSearcher, Query, ScoreDoc}
import org.apache.lucene.store.{Directory, FSDirectory}

import java.io.File
import java.nio.file.Path
import scala.util.Try

class LuceneDocsFinder(luceneIndex: String,
                       searchField: String) extends DocsFinder {
  private val indexPath: Path = new File(luceneIndex).toPath
  private val directory: Directory = FSDirectory.open(indexPath)
  private val ireader: DirectoryReader = DirectoryReader.open(directory)
  private val isearcher: IndexSearcher = new IndexSearcher(ireader)
  private val analyzer: NGAnalyzer = new NGAnalyzer(search = true)

  def findDocs(query: String,
               auxQuery: Option[String],
               maxDocs: Int = 100): Try[DocsProducer] = {
    Try {
      require(query != null)

      val parser: QueryParser = new QueryParser("", analyzer)
      val qur: Query = parser.parse(auxQuery match {
        case Some(aqry) => s"$searchField:($query) AND $aqry"
        case None => s"$searchField:($query)"
      })

      val hits: Array[ScoreDoc] = isearcher.search(qur, maxDocs).scoreDocs
      val ids: List[Int] = hits.map(sd => sd.doc).toList

      new DocsProducer() {
        def getDocuments: LazyList[Document] = lazyList(ids)
      }
    }
  }

  def getSearchField: Option[String] = Option(searchField)

  def close(): Try[Unit] = {
    Try {
      ireader.close()
      directory.close()
    }
  }

  private def lazyList(list: List[Int]): LazyList[Document] = list match {
    case h :: t => Tools.doc2doc(ireader.storedFields().document(h)) #:: lazyList(t)
    case Nil => LazyList[Document]()
  }
}
