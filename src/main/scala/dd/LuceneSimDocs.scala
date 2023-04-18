package dd

import dd.finders.LuceneDocsFinder
import dd.interfaces.{Comparator, Reporter}

class LuceneSimDocs(luceneIndex: String,
                    filters: Seq[Comparator],
                    reporters: Seq[Reporter],
                    auxQuery: Option[String],
                    searchField: String,
                    maxDocs: Int,
                    otherFields: Seq[String]) extends
  SimilarDocs(new LuceneDocsFinder(luceneIndex, searchField), filters, reporters, auxQuery, maxDocs, otherFields) {
}

