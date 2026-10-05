package dd.reporters

import dd.interfaces.{CompResult, Document, Reporter}

import java.io.Writer
import scala.util.Try

/**
 * Reporter that writes JSON comparison results to the process console.
 *
 * The JSON serialization and eligibility rules are delegated to
 * `JsonReporter`. Output is flushed after every accepted result and closing
 * this reporter does not close the process standard output stream.
 *
 * @param minTrue minimum number of similar comparator results required
 */
class ConsoleReporter(minTrue: Int = 0) extends Reporter:
  private val delegate: JsonReporter = new JsonReporter(
    new ConsoleWriter,
    minTrue,
    flushResults = true,
    prettyPrint = true
  )

  /**
   * Writes one eligible comparison result as JSON to the console.
   *
   * @param originalDoc source document used in the comparison
   * @param currentDoc candidate document being evaluated
   * @param otherFields additional field names to include in the output
   * @param results comparison results produced for the document pair
   * @return result of writing the comparison output
   */
  override def writeResults(originalDoc: Document,
                            currentDoc: Document,
                            otherFields: Seq[String],
                            results: Seq[CompResult]): Try[Unit] =
    delegate.writeResults(originalDoc, currentDoc, otherFields, results)

  /** Closes the JSON array and flushes the console without closing stdout. */
  override def close(): Try[Unit] = delegate.close()

  /** Writer adapter that keeps the process standard output open. */
  private final class ConsoleWriter extends Writer:
    /**
     * Writes a character buffer to standard output.
     *
     * @param cbuf character buffer
     * @param off first character offset
     * @param len number of characters to write
     */
    override def write(cbuf: Array[Char], off: Int, len: Int): Unit =
      System.out.print(String(cbuf, off, len))

    /** Flushes standard output. */
    override def flush(): Unit = System.out.flush()

    /** Flushes standard output without closing the process stream. */
    override def close(): Unit = flush()
