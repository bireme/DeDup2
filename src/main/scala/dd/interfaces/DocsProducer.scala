package dd.interfaces

/**
 * Contract for lazy document producers consumed by the comparison pipeline.
 *
 * Implementations expose documents as a `LazyList` so data can be streamed from
 * files, Lucene, or databases without materializing the whole source eagerly.
 */
trait DocsProducer:
  /**
   * Returns the produced documents.
   * @return lazy list of produced documents
   */
  def getDocuments: LazyList[Document]
