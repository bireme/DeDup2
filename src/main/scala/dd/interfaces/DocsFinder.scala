package dd.interfaces

import scala.util.Try

trait DocsFinder {
  def findDocs(query: String,
               auxQuery: Option[String],
               maxDocs: Int): Try[DocsProducer]

  def getSearchField: Option[String]

  def close(): Try[Unit]
}
