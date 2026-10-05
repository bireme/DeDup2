package dd

import dd.configurators.ConfMain
import dd.interfaces.{DocsProducer, Document}
import dd.tools.Tools
import dd.NGAnalyzer

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.jdk.CollectionConverters.*

class ConfMainLuceneProducerSuite extends munit.FunSuite:
  test("configuration parser creates a LuceneProducer"):
    val indexPath = Files.createTempDirectory("dedup2-conf-lucene-index")
    val configPath = Files.createTempFile("dedup2-conf-lucene", ".json")
    val reportPath = Files.createTempFile("dedup2-conf-lucene", ".pipe")
    val source: DocsProducer = new DocsProducer:
      /** Returns documents used to create the test index. */
      override def getDocuments: LazyList[Document] = LazyList(
        Document(Seq("id" -> "1", "title" -> "Alpha", "kind" -> "first")),
        Document(Seq("id" -> "2", "title" -> "Beta", "kind" -> "second"))
      )

    try
      Tools.createLuceneIndex(source, indexPath.toString, "title", new NGAnalyzer()).get
      val config =
        s"""{
           |  "producer": { "lucene": {
           |    "index": "${escape(indexPath.toString)}",
           |    "search": "alpha",
           |    "fields": ["id", "title"]
           |  } },
           |  "finder": { "lucene": {
           |    "index": "${escape(indexPath.toString)}",
           |    "searchField": "title",
           |    "minSimilarity": 0.7
           |  } },
           |  "comparators": [ { "exact": { "fieldName": "title", "normalize": true } } ],
           |  "reporters": [ { "pipe": {
           |    "file": "${escape(reportPath.toString)}",
           |    "encoding": "UTF-8",
           |    "recordSeparator": "|",
           |    "putHeader": false
           |  } } ]
           |}""".stripMargin
      Files.writeString(configPath, config, StandardCharsets.UTF_8)

      val parsed = ConfMain.parseSimilarDocsConfig(config)
      assertEquals(parsed.producer.getClass.getSimpleName, "LuceneProducer")
      assertEquals(parsed.producer.getDocuments.toList, List(
        Document(Seq("id" -> "1", "title" -> "Alpha"))
      ))
      parsed.finder.close().get
      parsed.reporters.foreach(_.reporter.close().get)
    finally
      Files.walk(indexPath).iterator().asScala.toSeq.reverse.foreach(Files.deleteIfExists)
      Files.deleteIfExists(configPath)
      Files.deleteIfExists(reportPath)

  private def escape(value: String): String =
    value.replace("\\", "\\\\").replace("\"", "\\\"")
