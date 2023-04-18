package dd.configurators

import dd.comparators.{AuthorsComparator, DiceComparator, ExactComparator, NGramComparator, RegexComparator}
import dd.finders.LuceneDocsFinder
import dd.interfaces.{Comparator, DocsFinder, Reporter}
import dd.reporters.PipeReporter
import play.api.libs.json.{JsArray, JsLookupResult, JsObject, JsString, JsValue, Json}

import java.io.{BufferedWriter, File}
import java.nio.charset.Charset
import java.nio.file.{Files, StandardOpenOption}
import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Success, Try}

/*
{
  "finder" : {
    "lucene" : {
      "index": ""
      "searchField": "",
    }
  }

  "comparators" : [
    "exact" : {
      "fieldName" : "",
      "normalize" : true
    },
    "dice" : {
      "fieldName" : "",
      "normalize" : true,
      "minSimilarity" : 80.5
    },
    "ngram" : {
      "fieldName" : "",
      "normalize" : true,
      "minSimilarity" : 80.5
    },
    "regex" : {
      "fieldName" : "",
      "normalize" : true,
      "regex" : "",
      "compString": ""
    },
    "authors" : {
      "fieldName" : ""
    }
  ],

  "reporters" : [
    "pipe" : {
      "file" : "",
      "encoding"; "",
      "recordSeparator" : "",
      "putHeader" : true,
      "minTrue" : 5
    }
  ]
}
*/

object ConfMain {
  def parseConfig(jsonFile: File): Try[(DocsFinder, Seq[Comparator], Seq[Reporter])] = {
    Try {
      val src: BufferedSource = Source.fromFile(jsonFile)
      val content: String = src.getLines().mkString("\n")

      src.close()
      parseConfig(content)
    }
  }

  private def parseConfig(jsonStr: String): (DocsFinder, Seq[Comparator], Seq[Reporter]) = {
    val json: JsValue = Json.parse(jsonStr)

    val lucene: JsLookupResult = json \ "finder" \ "lucene"
    val finder: DocsFinder = (lucene \ "index").get match {
      case index: JsString =>
        (lucene \ "searchField").get match {
          case searchField: JsString => new LuceneDocsFinder(index.value, searchField.value)
          case _ => throw new IllegalArgumentException("Missing 'finder/lucene/searchField")
        }
      case _ => throw new IllegalArgumentException("Missing 'finder/lucene/index")
    }

    val comparators: Seq[Comparator] = (json \ "comparators").get match {
      case arrayValue: JsArray => parseComparators(arrayValue, jsonStr)
      case _ => throw new IllegalArgumentException(s"Missing valid comparators: $jsonStr")
    }

    val reporters: Seq[Reporter] = (json \ "reporters").get match {
      case arrayValue: JsArray => parseReporters(arrayValue, jsonStr)
      case _ => throw new IllegalArgumentException(s"Missing valid reporters: $jsonStr")
    }

    (finder, comparators, reporters)
  }

  private def parseComparators(jarray: JsArray,
                               jsonStr: String): Seq[Comparator] = {
    jarray.as[Seq[JsObject]].map(_.value).foldLeft(Seq[Comparator]()) {
      case (seq, map) =>
        Try {
          if (map.contains("exact")) parseExactComparator(map("exact").as[JsObject])
          else if (map.contains("dice")) parseDiceComparator(map("dice").as[JsObject])
          else if (map.contains("ngram")) parseNGramComparator(map("ngram").as[JsObject])
          else if (map.contains("regex")) parseRegexComparator(map("regex").as[JsObject])
          else if (map.contains("authors")) parseAuthorsComparator(map("authors").as[JsObject])
          else throw new IllegalArgumentException(s"Invalid comparator: $jsonStr")
        } match {
          case Success(comparator) => seq :+ comparator
          case Failure(exception) =>
            throw new IllegalAccessException(s"Invalid comparator parameter: ${exception.getMessage}")
        }
    }
  }

  private def parseReporters(jarray: JsArray,
                             jsonStr: String): Seq[Reporter] = {
    jarray.as[Seq[JsObject]].map(_.value).foldLeft(Seq[Reporter]()) {
      case (seq, map) =>
        if (map.contains("pipe")) {
          Try(parsePipeReporter(map("pipe").as[JsObject])) match {
            case Success(reporter) => seq :+ reporter
            case Failure(exception) =>
              throw new IllegalAccessException(s"Invalid reporter parameter: ${exception.getMessage}")
          }
        }
        else throw new IllegalArgumentException(s"Invalid reporter: $jsonStr")
    }
  }

  private def parseExactComparator(json: JsObject): ExactComparator = {
    val map: collection.Map[String, JsValue] = json.value

    new ExactComparator(map("fieldName").asOpt[String].getOrElse(""), map("normalize").as[Boolean])
  }

  private def parseDiceComparator(json: JsObject): DiceComparator = {
    val map: collection.Map[String, JsValue] = json.value

    new DiceComparator(map("fieldName").asOpt[String].getOrElse(""), map("normalize").as[Boolean],
      map("minSimilarity").as[Double])
  }

  private def parseNGramComparator(json: JsObject): NGramComparator = {
    val map: collection.Map[String, JsValue] = json.value

    new NGramComparator(map("fieldName").asOpt[String].getOrElse(""), map("normalize").as[Boolean],
      map("minSimilarity").as[Double])
  }

  private def parseRegexComparator(json: JsObject): RegexComparator = {
    val map: collection.Map[String, JsValue] = json.value

    new RegexComparator(map("fieldName").asOpt[String].getOrElse(""), map("normalize").as[Boolean],
      map("regex").asOpt[String].getOrElse(""), map("compString").asOpt[String].getOrElse(""))
  }

  private def parseAuthorsComparator(json: JsObject): AuthorsComparator = {
    val map: collection.Map[String, JsValue] = json.value

    new AuthorsComparator(map("fieldName").asOpt[String].getOrElse(""))
  }

  private def parsePipeReporter(json: JsObject): PipeReporter = {
    val map: collection.Map[String, JsValue] = json.value
    val encoding: String = map("encoding").asOpt[String].getOrElse("")
    val writer: BufferedWriter = Files.newBufferedWriter(new File(map("file").asOpt[String].getOrElse("")).toPath,
      Charset.forName(encoding), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
    val minTrue: Int = map("minTrue").asOpt[Int].getOrElse(0)
    new PipeReporter(writer, map("recordSeparator").asOpt[String].getOrElse(""), map("putHeader").as[Boolean], minTrue)
  }
}
