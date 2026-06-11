package dd

import dd.interfaces.{DocsProducer, Document}
import dd.tools.Tools
import play.api.libs.json.{JsArray, JsNumber, JsString}

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.file.Files

class ToolsSuite extends munit.FunSuite:
  test("parseSqlFileList parses comma-separated SQL files"):
    assertEquals(
      Tools.parseSqlFileList("one.sql, two.sql,three.sql").get,
      Seq("one.sql", "two.sql", "three.sql")
    )

  test("doc2json groups repeated fields and parses embedded json values"):
    val document = Document(
      Seq(
        "title" -> "Example",
        "tag" -> "alpha",
        "tag" -> "beta",
        "metadata" -> """{"count":2}"""
      )
    )

    val json = Tools.doc2json(document)

    assertEquals((json \ "title").get, JsString("Example"))
    assertEquals((json \ "metadata" \ "count").get, JsNumber(2))
    assertEquals(
      (json \ "tag").get,
      JsArray(Seq(JsString("alpha"), JsString("beta")))
    )

  test("doc2json keeps singleton fields as arrays when requested"):
    val document = Document(Seq("title" -> "Example"))

    val json = Tools.doc2json(document, allFldsAreArray = true)

    assertEquals((json \ "title").get, JsArray(Seq(JsString("Example"))))

  test("createLuceneIndex reports title when indexed field is empty and id is empty"):
    val indexDir = Files.createTempDirectory("dedup-empty-field-index")
    val producer = new DocsProducer:
      override def getDocuments: LazyList[Document] =
        LazyList(Document(Seq("id" -> "", "title" -> "Fallback title", "abstract" -> "")))
    val errBytes = new ByteArrayOutputStream()
    val err = new PrintStream(errBytes)

    try
      Console.withErr(err):
        Tools.createLuceneIndex(producer, indexDir.toString, "abstract", new NGAnalyzer()).get
    finally err.close()

    assert(errBytes.toString("UTF-8").contains("Error indexing document field is empty. title=Fallback title"))
