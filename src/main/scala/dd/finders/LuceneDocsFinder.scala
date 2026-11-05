package dd.finders

import dd.NGAnalyzer
import dd.interfaces.{DocsFinder, DocsProducer, Document}
import dd.tools.StringSimilarity.LevenshteinJaccard
import dd.tools.Tools
import org.apache.lucene.document
import org.apache.lucene.analysis.TokenStream
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.Term
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{BooleanClause, BooleanQuery, ConstantScoreQuery, IndexSearcher, MatchNoDocsQuery, Query, ScoreDoc, TermQuery}
import org.apache.lucene.store.{Directory, FSDirectory}

import java.io.File
import java.io.StringReader
import java.nio.file.Path
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
 * Finder implementation backed by a Lucene index.
 *
 * The finder parses the incoming query against the configured search field,
 * executes the Lucene search, and exposes the matched stored documents through
 * a lazy producer consumed by the rest of the deduplication pipeline.
 */
class LuceneDocsFinder(luceneIndex: String,
                       searchField: String,
                       minSimilarity: Double) extends DocsFinder:
  require(minSimilarity >= 0.0 && minSimilarity <= 1.0)

  private val indexPath: Path = new File(luceneIndex).toPath
  private val directory: Directory = FSDirectory.open(indexPath)
  private val ireader: DirectoryReader = DirectoryReader.open(directory)
  private val isearcher: IndexSearcher = new IndexSearcher(ireader)
  private val analyzer: NGAnalyzer = new NGAnalyzer()
  private val maxQueryTokens: Int = 100

  /**
   * Finds the documents that match the given query.
   *
   * @param searchField field used to find the query
   * @param query main query string used to search for documents
   * @param auxQuery optional secondary query used to refine the search
   * @param maxDocs maximum number of documents to retrieve
   * @return result containing the produced documents
   */
  def findDocs(searchField: String,
               query: String,
               auxQuery: Option[String],
               maxDocs: Int = 1000): Try[DocsProducer] =
    findScoredDocs(searchField, query, auxQuery, maxDocs).map: scored =>
      new DocsProducer:
        /**
         * Returns the produced documents.
         * @return iterator of produced documents
         */
        def getDocuments: Iterator[Document] = scored.map(_.document)

  /**
   * Finds the documents that match the given query, keeping the Lucene score
   * and the similarity of the search field used to accept each candidate.
   *
   * @param searchField field used to find the query
   * @param query main query string used to search for documents
   * @param auxQuery optional secondary query used to refine the search
   * @param maxDocs maximum number of documents to retrieve
   * @return lazily converted candidates, in Lucene ranking order
   */
  def findScoredDocs(searchField: String,
                     query: String,
                     auxQuery: Option[String],
                     maxDocs: Int = 1000): Try[Iterator[ScoredDocument]] =
    Try:
      require(query != null)

      val parser: QueryParser = new QueryParser(searchField, analyzer)
      val qur: Query = buildQuery(searchField, query, auxQuery, parser)
      val hits: Array[ScoreDoc] = isearcher.search(qur, maxDocs).scoreDocs
      val normalizedQuery = Tools.normalizeWordsStr(query)

      lazyList(normalizedQuery, hits.iterator).iterator

  /** Builds the main Lucene query for a search request. */
  private def buildQuery(searchField: String,
                         query: String,
                         auxQuery: Option[String],
                         parser: QueryParser): Query =
    val tokenQuery: Query = buildTokenQuery(searchField, query)

    auxQuery match
      case Some(aqry) =>
        val builder: BooleanQuery.Builder = new BooleanQuery.Builder()
        builder.add(tokenQuery, BooleanClause.Occur.MUST)
        builder.add(parser.parse(aqry), BooleanClause.Occur.FILTER)
        builder.build()
      case None => tokenQuery

  /** Builds a query from analyzed input tokens. */
  private def buildTokenQuery(searchField: String,
                              query: String): Query =
    val tokens: Seq[String] = analyzedTokens(searchField, query).take(maxQueryTokens)
    if tokens.isEmpty then MatchNoDocsQuery("empty token query")
    else
      val builder: BooleanQuery.Builder = new BooleanQuery.Builder()
      builder.setMinimumNumberShouldMatch(1)

      tokens.foreach: token =>
        builder.add(ConstantScoreQuery(TermQuery(Term(searchField, token))), BooleanClause.Occur.SHOULD)

      builder.build()

  /**
   * Returns analyzer tokens for the supplied query text.
   *
   * @param searchField field whose analyzer is used
   * @param value text to tokenize
   * @return analyzed query tokens
   */
  private def analyzedTokens(searchField: String,
                             value: String): Seq[String] =
    val stream: TokenStream = analyzer.tokenStream(searchField, StringReader(value))
    val termAttr: CharTermAttribute = stream.addAttribute(classOf[CharTermAttribute])
    val tokens: ListBuffer[String] = ListBuffer.empty[String]

    try
      stream.reset()
      while stream.incrementToken() do
        tokens += termAttr.toString
      stream.end()
      tokens.toSeq
    finally stream.close()

  /**
   * Returns the configured search field, if any.
   * @return configured search field when available
   */
  def getSearchField: Option[String] = Some(searchField)

  /** Returns the configured Lucene similarity threshold. */
  def getMinSimilarity: Option[Double] = Some(minSimilarity)

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
   * @param normalizedQuery search query
   * @param iterator score documents already ranked by Lucene
   * @return lazy list of converted documents
   */
  private def lazyList(normalizedQuery: String,
                       iterator: Iterator[ScoreDoc]): LazyList[ScoredDocument] =
    Try:
      if iterator.hasNext then
        val hit: ScoreDoc = iterator.next()
        val doc: document.Document = ireader.storedFields().document(hit.doc)

        similarity(normalizedQuery, doc.get(searchField)) match
          case Some(value) if value >= minSimilarity =>
            ScoredDocument(Tools.doc2doc(doc), hit.score, value) #:: lazyList(normalizedQuery, iterator)
          case _ => lazyList(normalizedQuery, iterator)
      else LazyList[ScoredDocument]()
    .match
      case Success(ll) => ll
      case Failure(exception) =>
        exception.printStackTrace()
        LazyList[ScoredDocument]()

  /**
   * Computes the similarity between the source query and a candidate value.
   *
   * @param normalizedQuery normalized source query
   * @param candidate candidate field value
   * @return LevenshteinJaccard score, or None when either side is empty
   */
  private def similarity(normalizedQuery: String,
                         candidate: String): Option[Double] =
    val normalizedCandidate = Tools.normalizeWordsStr(Option(candidate).getOrElse(""))
    Option.when(normalizedQuery.nonEmpty && normalizedCandidate.nonEmpty):
      LevenshteinJaccard.score(normalizedQuery, normalizedCandidate)

/**
 * Candidate found by LuceneDocsFinder.
 *
 * @param document stored candidate document
 * @param score Lucene score of the hit
 * @param similarity LevenshteinJaccard similarity of the search field
 */
final case class ScoredDocument(document: Document,
                                score: Float,
                                similarity: Double)
