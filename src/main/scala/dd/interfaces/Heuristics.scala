package dd.interfaces

trait Heuristics:
  /** Determines whether comparator results identify a duplicate document. */

  def isDuplicated(doc: Document,
                   results: Seq[CompResult]): Boolean
