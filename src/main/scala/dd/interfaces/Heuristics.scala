package dd.interfaces

trait Heuristics:

  def isDuplicated(doc: Document,
                   results: Seq[CompResult]): Boolean
