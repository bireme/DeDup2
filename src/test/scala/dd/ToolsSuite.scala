package dd

import dd.interfaces.Document
import dd.tools.Tools
import play.api.libs.json.{JsArray, JsNumber, JsString}

class ToolsSuite extends munit.FunSuite:
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
