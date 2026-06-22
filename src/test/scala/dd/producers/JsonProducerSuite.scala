package dd.producers

import java.nio.charset.StandardCharsets
import java.nio.file.Files

class JsonProducerSuite extends munit.FunSuite:
  test("JsonProducer loads documents from a JSON file with mapped fields"):
    val jsonFile = Files.createTempFile("dedup2-json-producer", ".json")
    Files.writeString(
      jsonFile,
      """[
        |  {
        |    "dbase": "LILACS",
        |    "metadata": { "id": "1" },
        |    "title": "First row",
        |    "authors": [{ "name": "Ana" }, { "name": "Joao" }]
        |  },
        |  {
        |    "dbase": "MEDLINE",
        |    "metadata": { "id": "2" },
        |    "title": "Second row",
        |    "authors": []
        |  }
        |]""".stripMargin,
      StandardCharsets.UTF_8
    )

    val producer = new JsonProducer(
      input = jsonFile.toString,
      fields = Some(Seq(
        "dbase=dbase",
        "id=metadata.id",
        "title=title",
        "author=authors.name",
        "missing=metadata.missing"
      ))
    )

    val documents = producer.getDocuments.toList

    assertEquals(documents.size, 2)
    assertEquals(
      documents.head.fields,
      Seq(
        "dbase" -> "LILACS",
        "id" -> "1",
        "title" -> "First row",
        "author" -> "Ana¦Joao",
        "missing" -> ""
      )
    )
    assertEquals(
      documents.last.fields,
      Seq(
        "dbase" -> "MEDLINE",
        "id" -> "2",
        "title" -> "Second row",
        "author" -> "",
        "missing" -> ""
      )
    )

  test("JsonProducer loads a single document from a JSON string"):
    val producer = new JsonProducer("""{"dbase":"LILACS","id":123,"active":true,"title":"Sample"}""")

    val documents = producer.getDocuments.toList

    assertEquals(documents.size, 1)
    assertEquals(
      documents.head.fields,
      Seq(
        "dbase" -> "LILACS",
        "id" -> "123",
        "active" -> "true",
        "title" -> "Sample"
      )
    )

  test("JsonProducer skips non-object entries in JSON arrays"):
    val producer = new JsonProducer("""[{"id":"1"}, "invalid", {"id":"2"}]""")

    val documents = producer.getDocuments.toList

    assertEquals(documents.map(_.fields), Seq(Seq("id" -> "1"), Seq("id" -> "2")))

  test("parseFieldMapping requires output field and JSON path"):
    val valid = JsonProducer.parseFieldMapping("title=metadata.title").get
    val invalid = JsonProducer.parseFieldMapping("metadata.title")

    assertEquals(valid, JsonProducer.FieldMapping("title", Seq("metadata", "title")))
    assert(invalid.isFailure)
