package dd.interfaces

import scala.util.Try

/**
 * Contract for components that locate candidate documents for comparison.
 *
 * A finder receives the query extracted from a source document, resolves the
 * matching candidates from some backing store, and exposes them through a
 * `DocsProducer` so the rest of the pipeline can process them lazily.
 */
trait DocsFinder:
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
               maxDocs: Int): Try[DocsProducer]

  /**
   * Returns the configured search field, if any.
   * @return configured search field when available
   */
  def getSearchField: Option[String]

  /**
   * Closes the underlying resources.
   * @return result of closing the underlying resources
   */
  def close(): Try[Unit]
