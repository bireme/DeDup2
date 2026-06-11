package dd.reporters

import _root_.dd.NGAnalyzer
import _root_.dd.interfaces.{CompResult, Document}
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.FSDirectory

import java.nio.file.Files

class LuceneReporterSuite extends munit.FunSuite:
  test("writeResults stores renamed fields and indexes selected field with NGAnalyzer"):
    val indexDir = Files.createTempDirectory("dedup2-lucene-reporter")
    val reporter = new LuceneReporter(
      index = indexDir.toString,
      fieldToIndex = "title_for_search",
      fieldNameMapping = Map(
        "id_1" -> "original_id",
        "id_2" -> "current_id",
        "originalField" -> "title_for_search"
      ),
      minTrue = 1
    )
    val result = CompResult(
      "NGramComparator",
      "title",
      "São Paulo medicine",
      "Sao Paulo medicine",
      Some("saopaulomedicine"),
      Some("saopaulomedicine"),
      1.0,
      isSimilar = true
    )

    val written = reporter.writeResults(
      Document(Seq("id" -> "1", "title" -> "São Paulo medicine")),
      Document(Seq("id" -> "2", "title" -> "Sao Paulo medicine")),
      Seq("id"),
      Seq(result)
    )
    reporter.close().get

    assert(written.isSuccess)

    val directory = FSDirectory.open(indexDir)
    val reader = DirectoryReader.open(directory)
    val storedDoc = reader.storedFields().document(0)

    assertEquals(reader.numDocs(), 1)
    assertEquals(storedDoc.get("original_id"), "1")
    assertEquals(storedDoc.get("current_id"), "2")
    assertEquals(storedDoc.get("title_for_search"), "São Paulo medicine")
    assertEquals(storedDoc.get("currentField"), "Sao Paulo medicine")

    val analyzer = new NGAnalyzer(search = true)
    val query = new QueryParser("title_for_search", analyzer).parse("sao paulo")
    val hits = new IndexSearcher(reader).search(query, 10).scoreDocs

    assertEquals(hits.length, 1)

    analyzer.close()
    reader.close()
    directory.close()

  test("writeResults applies minTrue threshold"):
    val indexDir = Files.createTempDirectory("dedup2-lucene-reporter-mintrue")
    val reporter = new LuceneReporter(indexDir.toString, "originalField", minTrue = 2)
    val similarResult = CompResult("NGramComparator", "title", "a", "a", None, None, 1.0, isSimilar = true)
    val differentResult = CompResult("ExactComparator", "year", "2020", "2021", None, None, 0.0, isSimilar = false)

    reporter.writeResults(
      Document(Seq("id" -> "1")),
      Document(Seq("id" -> "2")),
      Seq("id"),
      Seq(similarResult, differentResult)
    ).get
    reporter.close().get

    val directory = FSDirectory.open(indexDir)
    val reader = DirectoryReader.open(directory)

    assertEquals(reader.numDocs(), 0)

    reader.close()
    directory.close()
