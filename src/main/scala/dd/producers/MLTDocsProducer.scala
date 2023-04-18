package dd.producers

import dd.interfaces.{DocsProducer, Document}
import dd.tools.Tools
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.queries.mlt.MoreLikeThis
import org.apache.lucene.search.spell.LevenshteinDistance
import org.apache.lucene.search.{IndexSearcher, Query, ScoreDoc, TopDocs}
import org.apache.lucene.store.FSDirectory

import java.io.StringReader
import java.nio.file.Paths
import scala.util.{Failure, Success, Try}

class MLTDocsProducer(indexPath: String,
                      fieldName: String,
                      matchContent: String,
                      minSimilarity: Option[Float]) extends DocsProducer {
//Try {
  private val directory: FSDirectory = FSDirectory.open(Paths.get(indexPath))
  private val reader: DirectoryReader = DirectoryReader.open(directory)
  private val searcher: IndexSearcher = new IndexSearcher(reader)
  private val mlt: MoreLikeThis = new MoreLikeThis(reader)
  mlt.setFieldNames(Array(fieldName))
  mlt.setMinTermFreq(1)
  mlt.setMinDocFreq(1)
  mlt.setAnalyzer(new StandardAnalyzer())

  private val query: Query = mlt.like(fieldName, new StringReader(matchContent))

  def getDocuments: LazyList[Document] = getDocumentsLazy(None, curPos = 0, query, searcher)

  private def getDocumentId(scoreDocs: Option[Array[ScoreDoc]],
                            curPos: Int,
                            query: Query,
                            searcher: IndexSearcher): Either[Throwable, (Array[ScoreDoc],Int)] = {
    scoreDocs match {
      case Some(arr) =>
        if (arr.isEmpty) Right(Array[ScoreDoc](), curPos)
        else if (curPos >= arr.length) {
          loadHits(Some(arr.last), query, searcher) flatMap {
            arr2 => getDocumentId(Some(arr2), curPos = 0, query, searcher)
          }
        } else Right((scoreDocs.get, curPos))
      case None =>
        loadHits(None, query, searcher) flatMap {
          arr2 => getDocumentId(Some(arr2), curPos = 0, query, searcher)
        }
      }
  }

  private def loadHits(after: Option[ScoreDoc],
                       query: Query,
                       searcher: IndexSearcher): Either[Throwable, Array[ScoreDoc]] = {
    val maxHits = 500

    Try {
      val topDocs: TopDocs = after match {
        case Some(aft) => searcher.searchAfter(aft, query, maxHits)
        case None => searcher.search(query, maxHits)
      }
      topDocs.scoreDocs
    }.toEither
  }

  private def getDocumentsLazy(after: Option[Array[ScoreDoc]],
                               curPos: Int,
                               query: Query,
                               searcher: IndexSearcher): LazyList[Document] = {
    getDocumentId(after, curPos, query, searcher) match {
      case Right((arr, cPos)) =>
        if (arr.isEmpty) LazyList.empty[Document]
        else {
          Try(searcher.storedFields().document(arr(cPos).doc)) match {
            case Success(ldoc) =>
              if (isSimilar(matchContent, ldoc, fieldName, minSimilarity.getOrElse(0))) {
                Try(Tools.doc2doc(ldoc)) match {
                  case Success(doc) => doc #:: getDocumentsLazy(Some(arr), cPos + 1, query, searcher)
                  case Failure(exception) =>
                    Console.println(s"MLTDocsProducer/getDocumentsLazy/${exception.getMessage}")
                    getDocumentsLazy(Some(arr), cPos + 1, query, searcher)
                }
              } else getDocumentsLazy(Some(arr), cPos + 1, query, searcher)
            case Failure(exception) =>
              Console.println(s"MLTDocsProducer/getDocumentsLazy/${exception.getMessage}")
              getDocumentsLazy(Some(arr), cPos + 1, query, searcher)
          }
        }
      case Left(exception) =>
        Console.println(s"MLTDocsProducer/getDocumentsLazy/${exception.getMessage}")
        LazyList.empty[Document]
    }
  }

  private def isSimilar(originalContent: String,
                        lucDocument: org.apache.lucene.document.Document,
                        fieldName: String,
                        minSimilarity: Float): Boolean = {
    val distance: LevenshteinDistance = new LevenshteinDistance()

    val maxDistance: Float = lucDocument.getFields(fieldName)
      .map(fld => distance.getDistance(fld.stringValue(), originalContent)).max

    maxDistance >= minSimilarity
  }
}
