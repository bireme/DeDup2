package dd.interfaces

import scala.util.Try

trait Reporter {
  def writeResults(originalDoc: Document,
                   currentDoc: Document,
                   otherFields: Seq[String],
                   results: Seq[CompResult]): Try[Unit]

  def close(): Try[Unit]
}
