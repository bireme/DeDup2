package dd.producers

import dd.interfaces.{DocsProducer, Document}
import dd.tools.Tools
import dd.NGAnalyzer

import java.nio.file.Files
import scala.jdk.CollectionConverters.*

class LuceneProducerSuite extends munit.FunSuite:
  test("LuceneProducer filters by search and returned fields"):
    val indexPath = Files.createTempDirectory("dedup2-lucene-producer")
    val source: DocsProducer = new DocsProducer:
      /** Returns the documents used to build the test index. */
      override def getDocuments: LazyList[Document] = LazyList(
        Document(Seq("id" -> "1", "title" -> "Alpha document", "extra" -> "keep out")),
        Document(Seq("id" -> "2", "title" -> "Beta document", "extra" -> "keep out"))
      )

    try
      assert(Tools.createLuceneIndex(source, indexPath.toString, "title", new NGAnalyzer()).isSuccess)

      val producer = new LuceneProducer(
        indexPath = indexPath.toString,
        search = Some("alpha"),
        fields = Some(Seq("id", "title"))
      )

      assertEquals(producer.getDocuments.toList, List(
        Document(Seq("id" -> "1", "title" -> "Alpha document"))
      ))
    finally
      Files.walk(indexPath).iterator().asScala.toSeq.reverse.foreach(Files.deleteIfExists)
