package dd.interfaces

trait DocsProducer {
  def getDocuments: LazyList[Document]
}