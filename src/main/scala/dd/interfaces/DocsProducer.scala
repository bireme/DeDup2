package dd.interfaces

/**
 * Contract for document producers consumed by the comparison pipeline.
 *
 * Implementations expose documents as an `Iterator` so data can be streamed from
 * files, Lucene, or databases without materializing the whole source eagerly.
 */
trait DocsProducer:
  /**
   * Returns the produced documents.
   * @return iterator of produced documents
   */
  def getDocuments: Iterator[Document]
