package dd.interfaces

/**
 * Immutable internal representation of a document handled by the pipeline.
 *
 * Each document is modeled as an ordered sequence of `(fieldName, value)`
 * pairs so repeated fields can be preserved while still allowing simple
 * transformations and serialization across the project.
 */
case class Document(fields: Seq[(String,String)])
