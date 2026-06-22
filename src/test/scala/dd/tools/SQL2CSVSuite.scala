package dd.tools

import dd.interfaces.Document

class SQL2CSVSuite extends munit.FunSuite:
  test("csvRecord uses configured field separator"):
    val record = SQL2CSV.csvRecord(Seq("1", "Title with comma, untouched", "ok"), '|')

    assertEquals(record, "1|Title with comma, untouched|ok")

  test("csvRecord quotes values containing the configured field separator"):
    val record = SQL2CSV.csvRecord(Seq("1", "Title with | separator", "ok"), '|')

    assertEquals(record, "1|\"Title with | separator\"|ok")

  test("documentToFields preserves document field order"):
    val document = Document(Seq(
      "author" -> "Ana",
      "title" -> "Sample",
      "id" -> "42",
      "publication_year" -> "2026"
    ))

    val fields = SQL2CSV.documentToFields(document)
    val fieldMap = fields.toMap

    assertEquals(fields.map(_._1), Seq("author", "title", "id", "publication_year"))
    assertEquals(fieldMap("title"), "Sample")

  test("documentToFields keeps first position for repeated fields and serializes them as json arrays"):
    val document = Document(Seq(
      "id" -> "42",
      "author" -> "Ana",
      "title" -> "Sample",
      "author" -> "Bea"
    ))

    val fields = SQL2CSV.documentToFields(document)
    val fieldMap = fields.toMap

    assertEquals(fields.map(_._1), Seq("id", "author", "title"))
    assertEquals(fieldMap("author"), """["Ana","Bea"]""")

  test("documentToFields preserves compact json object values"):
    val document = Document(Seq(
      "id" -> "42",
      "payload" -> """{"a":1,"b":"two"}"""
    ))

    val fields = SQL2CSV.documentToFields(document)

    assertEquals(fields, Seq("id" -> "42", "payload" -> """{"a":1,"b":"two"}"""))
