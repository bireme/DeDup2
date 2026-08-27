package dd.reporters

import dd.interfaces.CompResult

private[reporters] object ReporterSimilarityStatus:
  def displayValue(result: CompResult): String =
    if hasMaybeField(result) then "maybe" else result.isSimilar.toString

  private def hasMaybeField(result: CompResult): Boolean =
    isEmpty(result.originalField) ||
      isEmpty(result.currentField) ||
      result.originalFieldOther.exists(isEmpty) ||
      result.currentFieldOther.exists(isEmpty)

  private def isEmpty(value: String): Boolean =
    value == null || value.trim.isEmpty
