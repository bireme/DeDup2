package dd.interfaces

/**
 * Contract implemented by document comparators in the deduplication pipeline.
 *
 * A comparator inspects two documents and returns a structured result
 * describing the compared field values, their computed similarity, and the
 * boolean decision used by downstream reporters.
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
