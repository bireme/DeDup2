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
  private val delegate = new SimilarDocs(
    SimilarDocsConfig(
      producer = new DocsProducer:
        override def getDocuments: LazyList[Document] = LazyList.empty,
      finder = new LuceneDocsFinder(luceneIndex, searchField, minSimilarity),
      comparators = filters,
      reporters = reporters.map(reporter => ConfiguredReporter(reporter, otherFields)),
      auxQuery = auxQuery,
      maxDocs = maxDocs
    )
  )

  def processSimilars(originalDoc: Document): Try[Unit] =
    delegate.processSimilars(originalDoc)

  def close(): Try[Unit] =
    delegate.close()
