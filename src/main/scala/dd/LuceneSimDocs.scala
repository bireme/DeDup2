package dd

import dd.finders.LuceneDocsFinder
import dd.interfaces.{Comparator, Reporter}

/**
 * Convenience wrapper around `SimilarDocs` configured for a Lucene index.
 *
 * This class wires the Lucene-specific finder with the shared similarity
 * pipeline so callers can instantiate the comparison flow directly from Lucene
 * configuration parameters without assembling the dependencies manually.
 */
class LuceneSimDocs(luceneIndex: String,
                    filters: Seq[Comparator],
                    reporters: Seq[Reporter],
                    auxQuery: Option[String],
                    searchField: String,
                    maxDocs: Int,
                    otherFields: Seq[String])
    extends SimilarDocs(new LuceneDocsFinder(luceneIndex, searchField), filters, reporters, auxQuery, maxDocs, otherFields)
