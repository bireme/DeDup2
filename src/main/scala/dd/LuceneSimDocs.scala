package dd

import dd.configurators.ConfMain.{ConfiguredReporter, SimilarDocsConfig}
import dd.finders.LuceneDocsFinder
import dd.interfaces.{Comparator, Document, DocsProducer, Reporter}

import scala.util.Try

/**
 * Convenience wrapper around `SimilarDocs` configured for a Lucene index.
 *
 * This class wires the Lucene-specific finder with the shared similarity
 * pipeline so callers can instantiate the comparison flow directly from Lucene
 * configuration parameters without assembling a JSON file.
 */
class LuceneSimDocs(luceneIndex: String,
                    filters: Seq[Comparator],
                    reporters: Seq[Reporter],
                    auxQuery: Option[String],
                    searchField: String,
                    maxDocs: Option[Int],
                    otherFields: Seq[String],
                    minSimilarity: Double = 0.0):
  private val delegate: SimilarDocs = new SimilarDocs(
    SimilarDocsConfig(
      producer = new DocsProducer:
        /** Returns no source documents because this wrapper receives one directly. */
        override def getDocuments: LazyList[Document] = LazyList.empty,
      finder = new LuceneDocsFinder(luceneIndex, searchField, minSimilarity),
      comparators = filters,
      reporters = reporters.map(reporter => ConfiguredReporter(reporter, otherFields)),
      auxQuery = auxQuery,
      maxDocs = maxDocs
    )
  )

  /** Processes and reports similar documents for the supplied source document. */
  def processSimilars(originalDoc: Document): Try[Unit] =
    delegate.processSimilars(originalDoc)

  /** Closes the delegated similarity pipeline. */
  def close(): Try[Unit] =
    delegate.close()
