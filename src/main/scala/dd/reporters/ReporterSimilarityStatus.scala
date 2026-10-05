package dd.reporters

import dd.interfaces.CompResult

private[reporters] object ReporterSimilarityStatus:
  /**
   * Formats the similarity status for reporter output.
   *
   * @param result comparison result to format
   * @return displayable similarity status
   */
  def displayValue(result: CompResult): String =
    result.isSimilar.toString
