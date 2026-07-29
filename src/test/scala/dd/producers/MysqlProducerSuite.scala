package dd.producers

class MysqlProducerSuite extends munit.FunSuite:
  test("parseJsonFieldMappingLine uses SQL column name when output field is omitted"):
    assertEquals(
      MysqlProducer.parseJsonFieldMappingLine("author=text"),
      Some(("author", "text", "author"))
    )

    assertEquals(
      MysqlProducer.parseJsonFieldMappingLine(" author = text -> author.name "),
      Some(("author", "text", "author.name"))
    )

  test("getJsonSeq groups every object occurrence from a json array"):
    val json =
      """[
        |{"text":"Spanish title","_i":"es"},
        |{"text":"english title","_i":"en"},
        |{"text":"Portuguese title","_i":"pt"}
        |]""".stripMargin

    val variants = MysqlProducer.getJsonSeq(
      json,
      "author",
      Map("text" -> "author.name", "_i" -> "author.language")
    ).get

    assertEquals(
      variants.map(_.toMap),
      Seq(
        Map(
          "author.name" -> "english title//@//Portuguese title//@//Spanish title",
          "author.language" -> "en//@//es//@//pt"
        )
      )
    )

  test("getJsonSeq groups mapped JSON values with the SQL column name when output field is omitted"):
    val variants = MysqlProducer.getJsonSeq(
      """[{"text":"Portuguese title"},{"text":"english title"}]""",
      "author",
      Map("text" -> "author")
    ).get

    assertEquals(
      variants,
      Seq(
        Seq("author" -> "english title//@//Portuguese title")
      )
    )

  test("getJsonSeq creates one document variant per occurrence of the configured split field"):
    val variants = MysqlProducer.getJsonSeq(
      """[
        |{"text":"Spanish title"},
        |{"text":"Portuguese title"},
        |{"text":"English title"}
        |]""".stripMargin,
      "title",
      Map("text" -> "title"),
      splitDocumentField = Some("title")
    ).get

    assertEquals(
      variants,
      Seq(
        Seq("title" -> "Spanish title"),
        Seq("title" -> "Portuguese title"),
        Seq("title" -> "English title")
      )
    )

  test("getJsonSeq ignores empty mapped JSON values when grouping one array element"):
    val variants = MysqlProducer.getJsonSeq(
      """[{"_e":"","_f":"94"}]""",
      "pages",
      Map("_e" -> "pages", "_f" -> "pages")
    ).get

    assertEquals(
      variants,
      Seq(
        Seq("pages" -> "94")
      )
    )

  test("getJsonSeq groups arrays of non-objects with the SQL column name"):
    val variants = MysqlProducer.getJsonSeq(
      """["joao","Ana"]""",
      "author",
      Map("text" -> "author.name")
    ).get

    assertEquals(
      variants,
      Seq(
        Seq("author" -> "\"Ana\"//@//\"joao\"")
      )
    )

  test("getJsonSeq keeps plain text values from fields configured as JSON"):
    val variants = MysqlProducer.getJsonSeq(
      "Ultrasound",
      "subject",
      Map("text" -> "subject")
    ).get

    assertEquals(
      variants,
      Seq(
        Seq("subject" -> "Ultrasound")
      )
    )
