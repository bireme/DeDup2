package dd.tools

import org.bson.Document

class FixTranslationsSuite extends munit.FunSuite:
  test("removableFields removes ti_ia and ab_ia prefixed fields"):
    val document = new Document()
      .append("_id", "1")
      .append("ti_ia_en", "translated title")
      .append("ab_ia_en", "translated abstract")
      .append("ti_en", "original title")
      .append("ab_en", "original abstract")

    assertEquals(
      fixTranslations.removableFields(document).toSet,
      Set("ti_ia_en", "ab_ia_en")
    )

  test("removableFields keeps super_ab_ia field when corresponding ab field exists"):
    val document = new Document()
      .append("_id", "1")
      .append("ab_fr", "resume")
      .append("super_ab_ia_fr", "generated resume")

    assertEquals(fixTranslations.removableFields(document), Seq.empty)

  test("removableFields removes super_ab_ia field when corresponding ab field does not exist"):
    val document = new Document()
      .append("_id", "1")
      .append("ab_en", "abstract")
      .append("super_ab_ia_fr", "generated resume")
      .append("super_ab_ia_en", "generated abstract")

    assertEquals(
      fixTranslations.removableFields(document).toSet,
      Set("super_ab_ia_fr")
    )
