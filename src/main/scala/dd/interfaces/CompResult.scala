package dd.interfaces

/**
 * Immutable comparison result produced by a document comparator.
 *
 * The structure keeps both the original compared values and any derived
 * representations used during matching, together with the numeric similarity
 * score and the final boolean similarity decision.
 */
case class CompResult(name: String,
                      fieldName: String,
                      originalField: String,
                      currentField: String,
                      originalFieldOther: Option[String],
                      currentFieldOther: Option[String],
                      similarity: Double,
                      isSimilar: Boolean)
