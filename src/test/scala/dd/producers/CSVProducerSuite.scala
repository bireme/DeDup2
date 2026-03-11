package dd.producers

import java.nio.file.Files
import scala.jdk.CollectionConverters.SeqHasAsJava

class CSVProducerSuite extends munit.FunSuite:
  test("CSVProducer loads valid rows and skips malformed ones"):
    val csvFile = Files.createTempFile("dedup2-csv-producer", ".csv")
    Files.write(
      csvFile,
      Seq(
        "id,title",
        "1,First row",
        "2",
        "3,Third row"
      ).asJava
    )

    val producer = new CSVProducer(
      csvFile = csvFile.toString,
      schema = Map(0 -> "id", 1 -> "title"),
      hasHeader = true
    )

    val documents = producer.getDocuments.toList

    assertEquals(documents.size, 2)
    assertEquals(documents.head.fields, Seq("id" -> "1", "title" -> "First row"))
    assertEquals(documents.last.fields, Seq("id" -> "3", "title" -> "Third row"))
