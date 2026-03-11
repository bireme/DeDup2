package dd.configurators

import dd.NGAnalyzer
import dd.interfaces.{DocsProducer, Document}
import dd.tools.Tools

import java.nio.charset.StandardCharsets
import java.nio.file.Files

class ConfMainSuite extends munit.FunSuite:
  test("parseConfig builds finder, comparators, and reporters from json file"):
    val indexDir = Files.createTempDirectory("dedup2-confmain-index")
    val reportFile = Files.createTempFile("dedup2-confmain-report", ".csv")
    val configFile = Files.createTempFile("dedup2-confmain", ".json")

    val producer = new DocsProducer:
      override def getDocuments: LazyList[Document] =
        LazyList(Document(Seq("title" -> "sample", "id" -> "1")))

    Tools.createLuceneIndex(producer, indexDir.toString, "title", new NGAnalyzer()).get

    val config =
      s"""{
         |  "finder": {
         |    "lucene": {
         |      "index": "${escape(indexDir.toString)}",
         |      "searchField": "title"
         |    }
         |  },
         |  "comparators": [
         |    { "exact": { "fieldName": "title", "normalize": true } },
         |    { "dice": { "fieldName": "title", "normalize": true, "minSimilarity": 0.5 } }
         |  ],
         |  "reporters": [
         |    { "pipe": { "file": "${escape(reportFile.toString)}", "encoding": "UTF-8", "recordSeparator": "\\n", "putHeader": true, "minTrue": 1 } }
         |  ]
         |}""".stripMargin

    Files.writeString(configFile, config, StandardCharsets.UTF_8)

    val parsed = ConfMain.parseConfig(configFile.toFile).get

    assertEquals(parsed._2.size, 2)
    assertEquals(parsed._3.size, 1)
    assertEquals(parsed._1.getSearchField, Some("title"))

    parsed._1.close().get
    parsed._3.foreach(_.close().get)

  private def escape(path: String): String =
    path.replace("\\", "\\\\")
