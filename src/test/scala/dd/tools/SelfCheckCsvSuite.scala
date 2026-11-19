package dd.tools

import dd.SelfCheckDuplicated
import dd.producers.CSVProducer

import java.nio.charset.StandardCharsets
import java.nio.file.Files

/** CSV written by SQL2CSV and read by SelfCheckDuplicated with a configurable separator. */
class SelfCheckCsvSuite extends munit.FunSuite:
  private val header: Seq[String] = Seq("dbase", "id", "title", "author")
  private val row: Seq[String] =
    Seq("LILACS_Sas", "7", "Saúde | doença, \"um\" estudo", "Silva, Joao//@//Souza, Maria")

  test("parseCsvLine splits on the configured separator and honors quotes"):
    assertEquals(SelfCheckDuplicated.parseCsvLine("a|b|c", '|').get, Seq("a", "b", "c"))
    assertEquals(SelfCheckDuplicated.parseCsvLine("a,b|c", '|').get, Seq("a,b", "c"))
    assertEquals(SelfCheckDuplicated.parseCsvLine("\"a|b\"|c", '|').get, Seq("a|b", "c"))
    assertEquals(SelfCheckDuplicated.parseCsvLine("a,b|c", ',').get, Seq("a", "b|c"))
    assert(SelfCheckDuplicated.parseCsvLine("\"a|b", '|').isFailure)

  test("csvRecord quotes only values containing the separator, quotes or line breaks"):
    assertEquals(SQL2CSV.csvRecord(Seq("a,b", "c"), '|'), "a,b|c")
    assertEquals(SQL2CSV.csvRecord(Seq("a|b", "c"), '|'), "\"a|b\"|c")
    assertEquals(SQL2CSV.csvRecord(Seq("a,b", "c"), ','), "\"a,b\",c")

  Seq('|', ',').foreach: separator =>
    test(s"a record written with '$separator' is read back unchanged"):
      val file = Files.createTempFile("self-check-csv", ".csv")
      try
        val content = SQL2CSV.csvRecord(header, separator) + "\n" + SQL2CSV.csvRecord(row, separator) + "\n"
        Files.writeString(file, content, StandardCharsets.UTF_8)

        val parsedHeader = SelfCheckDuplicated.parseCsvLine(content.linesIterator.next(), separator).get
        assertEquals(parsedHeader, header)

        val schema = parsedHeader.zipWithIndex.map((field, index) => index -> field).toMap
        val documents = new CSVProducer(file.toString, schema, hasHeader = true, separator, "utf-8").getDocuments.toSeq
        assertEquals(documents.size, 1)
        assertEquals(documents.head.fields.toMap, header.zip(row).toMap)
      finally Files.deleteIfExists(file)

  test("CSVFileChecker.scanLine ignores separators inside quoted values"):
    assertEquals(CSVFileChecker.scanLine("a|b|c", '|', inQuotes = false), (2, false))
    assertEquals(CSVFileChecker.scanLine("a|\"b|c\"|d", '|', inQuotes = false), (2, false))
    assertEquals(CSVFileChecker.scanLine("a|\"say \"\"x|y\"\"\"|d", '|', inQuotes = false), (2, false))
    assertEquals(CSVFileChecker.scanLine("a||", '|', inQuotes = false), (2, false))
    assertEquals(CSVFileChecker.scanLine("a,b|c", ',', inQuotes = false), (1, false))
    // A quoted value with a line break: the record continues on the next line.
    assertEquals(CSVFileChecker.scanLine("a|\"first line|", '|', inQuotes = false), (1, true))
    assertEquals(CSVFileChecker.scanLine("second line\"|c", '|', inQuotes = true), (1, false))

  test("CSVFileChecker.scanLine counts the fields of every record written by csvRecord"):
    val values = Seq("LILACS_Sas", "Saúde | doença, \"um\" estudo", "linha 1\nlinha 2 | x", "")
    val record = SQL2CSV.csvRecord(values, '|')
    val (separators, quoted) = record.split("\n").foldLeft((0, false)):
      case ((count, inQuotes), line) =>
        val (lineCount, stillQuoted) = CSVFileChecker.scanLine(line, '|', inQuotes)
        (count + lineCount, stillQuoted)

    assertEquals(quoted, false)
    assertEquals(separators + 1, values.size)
