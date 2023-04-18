package dd.interfaces

abstract class Comparator {
  def compare(originalDoc: Document,
              currentDoc: Document): CompResult
}
