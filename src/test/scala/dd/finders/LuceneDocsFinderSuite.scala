package dd.finders

import dd.NGAnalyzer
import dd.interfaces.{DocsProducer, Document}
import dd.tools.Tools

import java.nio.file.Files

class LuceneDocsFinderSuite extends munit.FunSuite:
  test("findDocs lets Lucene order candidates by matching analyzer tokens in relative order"):
    val indexDir = Files.createTempDirectory("dedup2-lucene-finder-ranking")
    val producer = new DocsProducer:
      override def getDocuments: LazyList[Document] =
        LazyList(
          Document(Seq("id" -> "1", "title" -> "defzzzabc")),
          Document(Seq("id" -> "2", "title" -> "abczzzdef"))
        )

    Tools.createLuceneIndex(producer, indexDir.toString, "title", new NGAnalyzer()).get

    val finder = new LuceneDocsFinder(indexDir.toString, "title", minSimilarity = 0.0)
    val ids =
      try
        finder.findDocs("title", "abcxxxdef", None, maxDocs = 1).get
          .getDocuments
          .map(_.fields.collectFirst { case ("id", value) => value }.getOrElse(""))
          .toList
      finally finder.close().get

    assertEquals(ids, List("2"))

  test("findDocs recovers documents whose matching tokens have ordered gaps"):
    val indexDir = Files.createTempDirectory("dedup2-lucene-finder-ordered-gaps")
    val producer = new DocsProducer:
      override def getDocuments: LazyList[Document] =
        LazyList(
          Document(Seq("id" -> "1", "title" -> "xjfabcgegdefdsdsdgghi")),
          Document(Seq("id" -> "2", "title" -> "xjfghigegdefdsdsdgabc"))
        )

    Tools.createLuceneIndex(producer, indexDir.toString, "title", new NGAnalyzer()).get

    val finder = new LuceneDocsFinder(indexDir.toString, "title", minSimilarity = 0.0)
    val ids =
      try
        finder.findDocs("title", "abcdefghi", None, maxDocs = 10).get
          .getDocuments
          .map(_.fields.collectFirst { case ("id", value) => value }.getOrElse(""))
          .toList
      finally finder.close().get

    assert(ids.contains("1"))

  test("findDocs applies DiceCoefficient only after Lucene returns maxDocs"):
    val indexDir = Files.createTempDirectory("dedup2-lucene-finder-maxdocs-before-dice")
    val producer = new DocsProducer:
      override def getDocuments: LazyList[Document] =
        LazyList(
          Document(Seq("id" -> "1", "title" -> "abcdefxxxxxx")),
          Document(Seq("id" -> "2", "title" -> "abcdef"))
        )

    Tools.createLuceneIndex(producer, indexDir.toString, "title", new NGAnalyzer()).get

    val finder = new LuceneDocsFinder(indexDir.toString, "title", minSimilarity = 1.0)
    val ids =
      try
        finder.findDocs("title", "abcdef", None, maxDocs = 1).get
          .getDocuments
          .map(_.fields.collectFirst { case ("id", value) => value }.getOrElse(""))
          .toList
      finally finder.close().get

    assertEquals(ids, Nil)

  test("findDocs keeps DiceCoefficient filter for recovered documents"):
    val indexDir = Files.createTempDirectory("dedup2-lucene-finder-dice-filter")
    val producer = new DocsProducer:
      override def getDocuments: LazyList[Document] =
        LazyList(
          Document(Seq("id" -> "1", "title" -> "abcxxxdef")),
          Document(Seq("id" -> "2", "title" -> "abcdef"))
        )

    Tools.createLuceneIndex(producer, indexDir.toString, "title", new NGAnalyzer()).get

    val finder = new LuceneDocsFinder(indexDir.toString, "title", minSimilarity = 1.0)
    val ids =
      try
        finder.findDocs("title", "abcdef", None, maxDocs = 10).get
          .getDocuments
          .map(_.fields.collectFirst { case ("id", value) => value }.getOrElse(""))
          .toList
      finally finder.close().get

    assertEquals(ids, List("2"))

  test("findDocs keeps ordered query within Lucene nested clause limits"):
    val indexDir = Files.createTempDirectory("dedup2-lucene-finder-long-query")
    val title = (1 to 80).map(index => f"token$index%02d").mkString(" ")
    val producer = new DocsProducer:
      override def getDocuments: LazyList[Document] =
        LazyList(Document(Seq("id" -> "1", "title" -> title)))

    Tools.createLuceneIndex(producer, indexDir.toString, "title", new NGAnalyzer()).get

    val finder = new LuceneDocsFinder(indexDir.toString, "title", minSimilarity = 0.0)
    val ids =
      try
        finder.findDocs("title", title, None, maxDocs = 10).get
          .getDocuments
          .map(_.fields.collectFirst { case ("id", value) => value }.getOrElse(""))
          .toList
      finally finder.close().get

    assertEquals(ids, List("1"))
