package dd.finders

import dd.NGAnalyzer
import dd.interfaces.{DocsFinder, DocsProducer, Document}
import dd.tools.StringSimilarity.DiceCoefficient
import dd.tools.Tools
import org.apache.lucene.document
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.Term
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.queries.spans.{SpanNearQuery, SpanQuery, SpanTermQuery}
import org.apache.lucene.search.{BooleanClause, BooleanQuery, BoostQuery, ConstantScoreQuery, IndexSearcher, MatchNoDocsQuery, Query, ScoreDoc}
import org.apache.lucene.store.{Directory, FSDirectory}
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute

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
  private val orderedTokenSlop: Int = 1_000_000
  private val maxBooleanClauses: Int = 1024
  private val maxNestedTermClauses: Int = 900

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
    Try:
      require(query != null)

      val parser: QueryParser = new QueryParser(searchField, analyzer)
      val qur: Query = buildQuery(searchField, query, auxQuery, parser)
      val hits: Array[ScoreDoc] = isearcher.search(qur, maxDocs).scoreDocs
      val normalizedQuery = Tools.normalizeStr(query)
      val scoreDocs: Iterator[ScoreDoc] = hits.iterator

      new DocsProducer:
        /**
         * Returns the produced documents.
         * @return lazy list of produced documents
         */
        def getDocuments: LazyList[Document] = lazyList(normalizedQuery, scoreDocs)

  private def buildQuery(searchField: String,
                         query: String,
                         auxQuery: Option[String],
                         parser: QueryParser): Query =
    val positionalQuery = buildPositionalTokenQuery(searchField, query)

    auxQuery match
      case Some(aqry) =>
        val builder = new BooleanQuery.Builder()
        builder.add(positionalQuery, BooleanClause.Occur.MUST)
        builder.add(parser.parse(aqry), BooleanClause.Occur.FILTER)
        builder.build()
      case None => positionalQuery

  private def buildPositionalTokenQuery(searchField: String,
                                        query: String): Query =
    val tokens = analyzedTokens(searchField, query).take(maxNestedTermClauses)
    if tokens.isEmpty then MatchNoDocsQuery("empty positional token query")
    else
      val builder = new BooleanQuery.Builder()
      builder.setMinimumNumberShouldMatch(1)

      tokens.foreach: token =>
        builder.add(ConstantScoreQuery(spanTerm(searchField, token)), BooleanClause.Occur.SHOULD)

      val includeOrderedSequence = tokens.size > 1 && tokens.size * 2 <= maxNestedTermClauses
      val sequenceTermCost = if includeOrderedSequence then tokens.size else 0
      val pairLimitByNestedTerms = (maxNestedTermClauses - tokens.size - sequenceTermCost) / 2
      val pairLimitByBooleanClauses = maxBooleanClauses - tokens.size - (if includeOrderedSequence then 1 else 0)
      val orderedPairLimit = math.min(pairLimitByNestedTerms, pairLimitByBooleanClauses)

      orderedTokenPairs(tokens, orderedPairLimit).foreach:
        case (left, right) =>
          val orderedPair = orderedNearQuery(searchField, Seq(left, right))
          builder.add(BoostQuery(ConstantScoreQuery(orderedPair), 0.25f), BooleanClause.Occur.SHOULD)

      if includeOrderedSequence then
        val orderedSequence = orderedNearQuery(searchField, tokens)
        builder.add(
          BoostQuery(ConstantScoreQuery(orderedSequence), tokens.size.toFloat),
          BooleanClause.Occur.SHOULD
        )

      builder.build()

  private def orderedTokenPairs(tokens: Seq[String],
                                limit: Int): Seq[(String, String)] =
    if limit <= 0 then Seq.empty
    else
      val pairs = ListBuffer.empty[(String, String)]
      var gap = 1

      while gap < tokens.size && pairs.size < limit do
        var left = 0
        while left + gap < tokens.size && pairs.size < limit do
          pairs += (tokens(left) -> tokens(left + gap))
          left += 1
        gap += 1

      pairs.toSeq

  private def orderedNearQuery(searchField: String,
                               tokens: Seq[String]): SpanNearQuery =
    SpanNearQuery(
      tokens.map(token => spanTerm(searchField, token)).toArray,
      orderedTokenSlop,
      true
    )

  private def spanTerm(searchField: String,
                       token: String): SpanQuery =
    SpanTermQuery(Term(searchField, token))

  private def analyzedTokens(searchField: String,
                             value: String): Seq[String] =
    val stream = analyzer.tokenStream(searchField, StringReader(value))
    val termAttr = stream.addAttribute(classOf[CharTermAttribute])
    val tokens = ListBuffer.empty[String]

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
                       iterator: Iterator[ScoreDoc]): LazyList[Document] =
    Try:
      if iterator.hasNext then
        val doc: document.Document = ireader.storedFields().document(iterator.next().doc)

        if isSimilar(normalizedQuery, doc.get(searchField)) then
          Tools.doc2doc(doc) #:: lazyList(normalizedQuery, iterator)
        else lazyList(normalizedQuery, iterator)
      else LazyList[Document]()
    .match
      case Success(ll) => ll
      case Failure(exception) =>
        exception.printStackTrace()
        LazyList[Document]()

  private def isSimilar(normalizedQuery: String,
                        candidate: String): Boolean =
    val normalizedCandidate = Tools.normalizeStr(Option(candidate).getOrElse(""))
    normalizedQuery.nonEmpty && normalizedCandidate.nonEmpty &&
      DiceCoefficient.score(normalizedQuery, normalizedCandidate) >= minSimilarity
