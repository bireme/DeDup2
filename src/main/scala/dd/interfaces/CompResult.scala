package dd.interfaces

case class CompResult(name: String,
                      fieldName: String,
                      originalField: String,
                      currentField: String,
                      originalFieldOther: Option[String],
                      currentFieldOther: Option[String],
                      similarity: Double,
                      isSimilar: Boolean)
