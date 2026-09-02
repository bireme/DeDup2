package dd.reporters

import dd.interfaces.CompResult

private[reporters] object ReporterSimilarityStatus:
  /** Formats the similarity status for reporter output. */
  def displayValue(result: CompResult): String =
    if hasMaybeField(result) then "maybe" else result.isSimilar.toString

  /** Indicates whether a comparator result contains an indeterminate value. */
  private def hasMaybeField(result: CompResult): Boolean =
    isEmpty(result.originalField) ||
      isEmpty(result.currentField) ||
      result.originalFieldOther.exists(isEmpty) ||
      result.currentFieldOther.exists(isEmpty)

  /** Tests whether a result value is blank or represents null. */
  private def isEmpty(value: String): Boolean =
    value == null || value.trim.isEmpty
