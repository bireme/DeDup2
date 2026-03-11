package dd.interfaces

import scala.util.Try

/**
 * Contract for output reporters in the deduplication pipeline.
 *
 * Reporters receive each document pair together with the comparison results and
 * are responsible for persisting, serializing, or exporting the resulting
 * information to the chosen destination.
 */
trait Reporter:
  /**
   * Writes the comparison results.
   *
   * @param originalDoc source document used in the comparison
   * @param currentDoc candidate document being evaluated
   * @param otherFields additional field names to include in the output
   * @param results comparison results produced for the document pair
   * @return result of writing the comparison output
   */
  def writeResults(originalDoc: Document,
                   currentDoc: Document,
                   otherFields: Seq[String],
                   results: Seq[CompResult]): Try[Unit]

  /**
   * Closes the underlying resources.
   * @return result of closing the underlying resources
   */
  def close(): Try[Unit]
