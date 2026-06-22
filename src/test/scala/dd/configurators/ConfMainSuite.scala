package dd.configurators

import dd.NGAnalyzer
import dd.configurators.ConfMain.{SelfCheckCsvSourceConfig, SelfCheckMysqlSourceConfig}
import dd.interfaces.{CompResult, DocsProducer, Document}
import dd.tools.Tools
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.store.FSDirectory

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
         |      "searchField": "title",
         |      "minSimilarity": 0.8
         |    }
         |  },
         |  "comparators": [
         |    { "exact": { "fieldName": "title", "normalize": true } },
         |    { "dice": { "fieldName": "title", "normalize": true, "minSimilarity": 0.5 } }
         |  ],
         |  "reporters": [
         |    { "pipe": { "file": "${escape(reportFile.toString)}", "encoding": "UTF-8", "recordSeparator": "|", "putHeader": true, "flushResults": true } }
         |  ]
         |}""".stripMargin

    Files.writeString(configFile, config, StandardCharsets.UTF_8)

    val parsed = ConfMain.parseConfig(configFile.toFile).get

    assertEquals(parsed._2.size, 2)
    assertEquals(parsed._3.size, 1)
    assertEquals(parsed._1.getSearchField, Some("title"))

    val result = CompResult("ExactComparator", "title", "sample", "sample", None, None, 1.0, isSimilar = true)
    parsed._3.head.writeResults(
      Document(Seq("id" -> "1", "title" -> "sample")),
      Document(Seq("id" -> "2", "title" -> "sample")),
      Seq("id"),
      Seq(result)
    ).get
    parsed._3.head.writeResults(
      Document(Seq("id" -> "1", "title" -> "sample")),
      Document(Seq("id" -> "3", "title" -> "sample")),
      Seq("id"),
      Seq(result)
    ).get

    val lines = Files.readAllLines(reportFile, StandardCharsets.UTF_8)
    assertEquals(lines.size, 3)

    parsed._1.close().get
    parsed._3.foreach(_.close().get)

  test("parseConfig accepts MongoDB reporter without port"):
    val indexDir = Files.createTempDirectory("dedup2-confmain-mongo-index")
    val configFile = Files.createTempFile("dedup2-confmain-mongo", ".json")

    val producer = new DocsProducer:
      override def getDocuments: LazyList[Document] =
        LazyList(Document(Seq("title" -> "sample", "id" -> "1")))

    Tools.createLuceneIndex(producer, indexDir.toString, "title", new NGAnalyzer()).get

    val config =
      s"""{
         |  "finder": {
         |    "lucene": {
         |      "index": "${escape(indexDir.toString)}",
         |      "searchField": "title",
         |      "minSimilarity": 0.8
         |    }
         |  },
         |  "comparators": [
         |    { "exact": { "fieldName": "title", "normalize": true } }
         |  ],
         |  "reporters": [
         |    { "mongoDB": { "database": "dedup2", "collection": "results", "append": true, "host": "127.0.0.1:27017", "minTrue": 2 } }
         |  ]
         |}""".stripMargin

    Files.writeString(configFile, config, StandardCharsets.UTF_8)

    val parsed = ConfMain.parseConfig(configFile.toFile).get

    assertEquals(parsed._3.size, 1)

    parsed._1.close().get
    parsed._3.foreach(_.close().get)

  test("parseSimilarDocsConfig reads document parallelism"):
    val indexDir = Files.createTempDirectory("dedup2-confmain-parallel-index")
    val reportFile = Files.createTempFile("dedup2-confmain-parallel-report", ".csv")
    val configFile = Files.createTempFile("dedup2-confmain-parallel", ".json")
    val csvFile = Files.createTempFile("dedup2-confmain-parallel", ".csv")
    val schemaFile = Files.createTempFile("dedup2-confmain-parallel-schema", ".txt")

    val producer = new DocsProducer:
      override def getDocuments: LazyList[Document] =
        LazyList(Document(Seq("title" -> "sample", "id" -> "1")))

    Tools.createLuceneIndex(producer, indexDir.toString, "title", new NGAnalyzer()).get
    Files.writeString(csvFile, "dbase,id,title\nLILACS,1,sample\n", StandardCharsets.UTF_8)
    Files.writeString(schemaFile, "0=dbase,1=id,2=title", StandardCharsets.UTF_8)

    val config =
      s"""{
         |  "documentParallelism": 3,
         |  "producer": {
         |    "csv": {
         |      "file": "${escape(csvFile.toString)}",
         |      "schema": "file=${escape(schemaFile.toString)}",
         |      "hasHeader": true
         |    }
         |  },
         |  "finder": {
         |    "lucene": {
         |      "index": "${escape(indexDir.toString)}",
         |      "searchField": "title",
         |      "minSimilarity": 0.8
         |    }
         |  },
         |  "comparators": [
         |    { "exact": { "fieldName": "title", "normalize": true } }
         |  ],
         |  "reporters": [
         |    { "pipe": { "file": "${escape(reportFile.toString)}", "encoding": "UTF-8", "recordSeparator": "|", "putHeader": true } }
         |  ]
         |}""".stripMargin

    Files.writeString(configFile, config, StandardCharsets.UTF_8)

    val parsed = ConfMain.parseSimilarDocsConfig(configFile.toFile).get

    assertEquals(parsed.documentParallelism, 3)
    parsed.finder.close().get
    parsed.reporters.foreach(_.reporter.close().get)

  test("parseConfig accepts Lucene reporter with field name mapping"):
    val sourceIndexDir = Files.createTempDirectory("dedup2-confmain-lucene-source")
    val reportIndexDir = Files.createTempDirectory("dedup2-confmain-lucene-report")
    val configFile = Files.createTempFile("dedup2-confmain-lucene", ".json")

    val producer = new DocsProducer:
      override def getDocuments: LazyList[Document] =
        LazyList(Document(Seq("title" -> "sample", "id" -> "1")))

    Tools.createLuceneIndex(producer, sourceIndexDir.toString, "title", new NGAnalyzer()).get

    val config =
      s"""{
         |  "finder": {
         |    "lucene": {
         |      "index": "${escape(sourceIndexDir.toString)}",
         |      "searchField": "title",
         |      "minSimilarity": 0.8
         |    }
         |  },
         |  "comparators": [
         |    { "exact": { "fieldName": "title", "normalize": true } }
         |  ],
         |  "reporters": [
         |    {
         |      "lucene": {
         |        "index": "${escape(reportIndexDir.toString)}",
         |        "fieldToIndex": "title_for_search",
         |        "fieldNameMapping": {
         |          "id_1": "original_id",
         |          "originalField": "title_for_search"
         |        },
         |        "minTrue": 1
         |      }
         |    }
         |  ]
         |}""".stripMargin

    Files.writeString(configFile, config, StandardCharsets.UTF_8)

    val parsed = ConfMain.parseConfig(configFile.toFile).get
    val result = CompResult("ExactComparator", "title", "sample", "sample", None, None, 1.0, isSimilar = true)

    parsed._3.head.writeResults(
      Document(Seq("id" -> "1", "title" -> "sample")),
      Document(Seq("id" -> "2", "title" -> "sample")),
      Seq("id"),
      Seq(result)
    ).get

    parsed._1.close().get
    parsed._3.foreach(_.close().get)

    val directory = FSDirectory.open(reportIndexDir)
    val reader = DirectoryReader.open(directory)
    val storedDoc = reader.storedFields().document(0)

    assertEquals(parsed._3.size, 1)
    assertEquals(reader.numDocs(), 1)
    assertEquals(storedDoc.get("original_id"), "1")
    assertEquals(storedDoc.get("title_for_search"), "sample")

    reader.close()
    directory.close()

  test("parseConfig ignores finder lucene ngram and uses lucene minSimilarity"):
    val indexDir = Files.createTempDirectory("dedup2-confmain-minsim-index")
    val reportFile = Files.createTempFile("dedup2-confmain-minsim-report", ".csv")
    val configFile = Files.createTempFile("dedup2-confmain-minsim", ".json")

    val producer = new DocsProducer:
      override def getDocuments: LazyList[Document] =
        LazyList(
          Document(Seq("title" -> "Sample Title", "id" -> "1")),
          Document(Seq("title" -> "Completely Different", "id" -> "2"))
        )

    Tools.createLuceneIndex(producer, indexDir.toString, "title", new NGAnalyzer()).get

    val config =
      s"""{
         |  "finder": {
         |    "lucene": {
         |      "index": "${escape(indexDir.toString)}",
         |      "searchField": "title",
         |      "minSimilarity": 1.0,
         |      "ngram": {
         |        "normalize": false,
         |        "minSimilarity": 0.0
         |      }
         |    }
         |  },
         |  "comparators": [
         |    { "exact": { "fieldName": "title", "normalize": true } }
         |  ],
         |  "reporters": [
         |    { "pipe": { "file": "${escape(reportFile.toString)}", "encoding": "UTF-8", "recordSeparator": "|", "putHeader": true } }
         |  ]
         |}""".stripMargin

    Files.writeString(configFile, config, StandardCharsets.UTF_8)

    val parsed = ConfMain.parseConfig(configFile.toFile).get
    val matches = parsed._1.findDocs("title", "sample title", None, 10).get.getDocuments.toList

    assertEquals(matches.size, 1)
    assertEquals(matches.head.fields.collectFirst { case ("id", value) => value }, Some("1"))

    parsed._1.close().get
    parsed._3.foreach(_.close().get)

  test("parseSelfCheckDuplicatedConfig reads MySQL producer without opening a connection"):
    val reportFile = Files.createTempFile("dedup2-selfcheck-report", ".csv")
    val configFile = Files.createTempFile("dedup2-selfcheck", ".json")
    val jsonFieldFile = Files.createTempFile("dedup2-json-fields", ".txt")

    Files.writeString(jsonFieldFile, "author=text\ntitle=text->article_title\n", StandardCharsets.UTF_8)

    val config =
      s"""{
         |  "producer": {
         |    "mysql": {
         |      "host": "db.example.org",
         |      "port": 3307,
         |      "dbnm": "dedup",
         |      "user": "reader",
         |      "pswd": "secret",
         |      "sqls": "sql/sample.sql",
         |      "sqlEncoding": "iso8859-1",
         |      "jsonFieldFile": "${escape(jsonFieldFile.toString)}"
         |    }
         |  },
         |  "finder": {
         |    "lucene": {
         |      "index": "ignored-by-self-check",
         |      "searchField": "title",
         |      "minSimilarity": 0.8,
         |      "auxQuery": "dbase:LILACS",
         |      "maxDocs": 20
         |    }
         |  },
         |  "comparators": [
         |    { "exact": { "fieldName": "title", "normalize": true } }
         |  ],
         |  "reporters": [
         |    { "pipe": { "file": "${escape(reportFile.toString)}", "encoding": "UTF-8", "recordSeparator": "|", "putHeader": true, "otherFields": ["title"] } }
         |  ],
         |  "selfCheckDuplicated": {
         |    "outCsvFile": "self.csv",
         |    "index": "self-index",
         |    "encoding": "utf-8"
         |  }
         |}""".stripMargin

    Files.writeString(configFile, config, StandardCharsets.UTF_8)

    val parsed = ConfMain.parseSelfCheckDuplicatedConfig(configFile.toFile).get

    val mysql = parsed.source match
      case SelfCheckMysqlSourceConfig(value) => value
      case other => fail(s"Expected MySQL source, got $other")

    assertEquals(mysql.mySqlHost, "db.example.org")
    assertEquals(mysql.mySqlPort, 3307)
    assertEquals(mysql.mySqlDbname, "dedup")
    assertEquals(mysql.sqlfs, Seq("sql/sample.sql"))
    assertEquals(mysql.sqlf, "sql/sample.sql")
    assertEquals(mysql.sqlEncoding, "iso8859-1")
    assertEquals(mysql.jsonFields, Some(Map("author" -> Map("text" -> "author"), "title" -> Map("text" -> "article_title"))))
    assertEquals(parsed.searchField, "title")
    assertEquals(parsed.minSimilarity, 0.8)
    assertEquals(parsed.auxQuery, Some("dbase:LILACS"))
    assertEquals(parsed.maxDocs, Some(20))
    assertEquals(parsed.outCsvFile, Some("self.csv"))
    assertEquals(parsed.index, Some("self-index"))
    assertEquals(parsed.comparators.size, 1)
    assertEquals(parsed.reporters.size, 1)
    assertEquals(parsed.reporters.head.otherFields, Seq("dbase", "id", "title"))

    parsed.reporters.foreach(_.reporter.close().get)

  test("parseSelfCheckDuplicatedConfig creates parent directories for pipe reporter files"):
    val baseDir = Files.createTempDirectory("dedup2-selfcheck-report-parent")
    val reportFile = baseDir.resolve("missing").resolve("report.txt")
    val configFile = Files.createTempFile("dedup2-selfcheck-missing-parent", ".json")

    val config =
      s"""{
         |  "producer": {
         |    "csv": {
         |      "file": "ignored.csv",
         |      "schema": "0=dbase,1=id,2=title",
         |      "hasHeader": true,
         |      "fieldSeparator": ";"
         |    }
         |  },
         |  "finder": {
         |    "lucene": {
         |      "index": "ignored-by-self-check",
         |      "searchField": "title",
         |      "minSimilarity": 0.8
         |    }
         |  },
         |  "comparators": [
         |    { "exact": { "fieldName": "title", "normalize": true } }
         |  ],
         |  "reporters": [
         |    { "pipe": { "file": "${escape(reportFile.toString)}", "encoding": "UTF-8", "recordSeparator": "|", "putHeader": true } }
         |  ]
         |}""".stripMargin

    Files.writeString(configFile, config, StandardCharsets.UTF_8)

    val parsed = ConfMain.parseSelfCheckDuplicatedConfig(configFile.toFile).get

    assert(Files.isDirectory(reportFile.getParent))
    assert(Files.exists(reportFile))

    parsed.reporters.foreach(_.reporter.close().get)

  test("parseSelfCheckDuplicatedConfig accepts CSV producer"):
    val csvFile = Files.createTempFile("dedup2-selfcheck-source", ".csv")
    val reportFile = Files.createTempFile("dedup2-selfcheck-csv-report", ".csv")
    val configFile = Files.createTempFile("dedup2-selfcheck-csv", ".json")

    Files.writeString(csvFile, "dbase;id;title\nLILACS;1;sample\n", StandardCharsets.UTF_8)

    val config =
      s"""{
         |  "producer": {
         |    "csv": {
         |      "file": "${escape(csvFile.toString)}",
         |      "schema": "0=dbase,1=id,2=title",
         |      "hasHeader": true,
         |      "fieldSeparator": ";",
         |      "encoding": "UTF-8"
         |    }
         |  },
         |  "finder": {
         |    "lucene": {
         |      "index": "ignored-by-self-check",
         |      "searchField": "title",
         |      "minSimilarity": 0.8
         |    }
         |  },
         |  "comparators": [
         |    { "exact": { "fieldName": "title", "normalize": true } }
         |  ],
         |  "reporters": [
         |    { "pipe": { "file": "${escape(reportFile.toString)}", "encoding": "UTF-8", "recordSeparator": "|", "putHeader": true } }
         |  ]
         |}""".stripMargin

    Files.writeString(configFile, config, StandardCharsets.UTF_8)

    val parsed = ConfMain.parseSelfCheckDuplicatedConfig(configFile.toFile).get
    val csv = parsed.source match
      case SelfCheckCsvSourceConfig(value) => value
      case other => fail(s"Expected CSV source, got $other")

    assertEquals(csv.csvFile, csvFile.toString)
    assertEquals(csv.schema, Map(0 -> "dbase", 1 -> "id", 2 -> "title"))
    assertEquals(csv.hasHeader, true)
    assertEquals(csv.fieldSeparator, ';')
    assertEquals(csv.encoding, "UTF-8")
    assertEquals(parsed.searchField, "title")
    assertEquals(parsed.minSimilarity, 0.8)
    assertEquals(parsed.comparators.size, 1)
    assertEquals(parsed.reporters.size, 1)

    parsed.reporters.foreach(_.reporter.close().get)

  private def escape(path: String): String =
    path.replace("\\", "\\\\")
