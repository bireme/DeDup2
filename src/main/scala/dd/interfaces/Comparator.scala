package dd.interfaces

/**
 * Contract implemented by document comparators in the deduplication pipeline.
 *
 * A comparator inspects two documents and returns a structured result
 * describing the compared field values, their computed similarity.
 */
abstract class Comparator:
  /**
   * Compares the input documents and returns the comparison result.
   *
   * @param originalDoc source document used in the comparison
   * @param currentDoc candidate document being evaluated
   * @return comparison result describing the evaluated documents
   */
  def compare(originalDoc: Document,
              currentDoc: Document): CompResult

  /**
   * Prepares the comparator for a fixed source document.
   *
   * <p>The pipeline compares one source document against many candidates, so
   * comparators may override this method to process the source document only
   * once. The returned comparator must produce exactly the same results as
   * {@code compare(originalDoc, currentDoc)}. The default implementation just
   * delegates to {@code compare}.</p>
   *
   * @param originalDoc source document used in every subsequent comparison
   * @return comparator bound to the source document
   */
  def prepare(originalDoc: Document): PreparedComparator =
    currentDoc => compare(originalDoc, currentDoc)

/** Comparator bound to a source document by {@link Comparator.prepare}. */
trait PreparedComparator:
  /**
   * Compares the bound source document with a candidate document.
   *
   * @param currentDoc candidate document being evaluated
   * @return comparison result describing the evaluated documents
   */
  def compare(currentDoc: Document): CompResult
