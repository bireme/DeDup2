package dd.tools

class SQL2CSVSuite extends munit.FunSuite:
  test("csvRecord uses configured field separator"):
    val record = SQL2CSV.csvRecord(Seq("1", "Title with comma, untouched", "ok"), '|')

    assertEquals(record, "1|Title with comma, untouched|ok")

  test("csvRecord quotes values containing the configured field separator"):
    val record = SQL2CSV.csvRecord(Seq("1", "Title with | separator", "ok"), '|')

    assertEquals(record, "1|\"Title with | separator\"|ok")
