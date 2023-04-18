package dd

import dd.configurators.ConfMain
import dd.interfaces.{CompResult, Comparator, DocsFinder, DocsProducer, Document, Reporter}
import dd.producers.CSVProducer

import java.io.File
import scala.util.{Failure, Success, Try}

class SimilarDocs(finder: DocsFinder,
                  filters: Seq[Comparator],
                  reporters: Seq[Reporter],
                  auxQuery: Option[String],
                  maxDocs: Int,
                  otherFields: Seq[String]) {

  def processSimilars(originalDoc: Document): Try[Unit] = {
    similar(originalDoc) map {
      list =>
        list.foreach {
          case (currentDoc, results) =>
            reporters.foreach {
              _.writeResults(originalDoc, currentDoc, otherFields, results)
            }
        }
    }
  }

  def close(): Try[Unit] = Try {
    finder.close()
    reporters.foreach(_.close())
  }

  private def similar(originalDoc: Document): Try[LazyList[(Document, Seq[CompResult])]] = {
    val query = finder.getSearchField match {
      case Some(srcField) =>
        originalDoc.fields.filter(_._1.equals(srcField)).map(_._2).headOption match {
          case Some(qry) => qry
          case None => throw new Exception("Empty search field")
        }
      case None => throw new Exception("Empty search field")
    }
    finder.findDocs(query, auxQuery, maxDocs).map {
      producer: DocsProducer => getResults(originalDoc, producer.getDocuments)
    }
  }

  private def getResults(originalDoc: Document,
                         docs: LazyList[Document]): LazyList[(Document, Seq[CompResult])] = {
    docs.headOption match {
      case Some(doc) => getResults(originalDoc, doc) #:: getResults(originalDoc, docs.tail)
      case None => LazyList.empty[(Document, Seq[CompResult])]
    }
  }

  private def getResults(originalDoc: Document,
                         currentDoc: Document): (Document, Seq[CompResult]) = {
    (currentDoc, filters.foldLeft(Seq[CompResult]()) {
      case (seq, filter) => seq :+ filter.compare(originalDoc, currentDoc)
    })
  }
}

object SimilarDocs extends App {
  private def usage(): Unit = {
    System.err.println("Check for duplicated documents in a database/index.")
    System.err.println("\nusage: SimilarDocs <options>")
    System.err.println("\n<options>:")
    System.err.println("\t-inputPipeFile=<path> Input pipe document file. Contain documents(one per line) used to look for similar ones.")
    System.err.println("\t-schema=<pos>:<fieldName>,...,<pos>:<fieldName> Associate the csv field position (starting from 0)")
    System.err.println("\t\twith the Lucene document field's name. Only the fields present in the confFile.")
    System.err.println("\t-confFile=<path>      Configuration file. See documentation for configuration file description.")
    System.err.println("\t[-otherFields=<field1>,<field2>,...,<fieldN>]  Field names  not present in the schema but that")
    System.err.println("\t\tshould be included in the output report.")
    System.err.println("\t[-auxQuery=<str>]     Auxiliary query used to complement the one used from searchField.")
    System.err.println("\t[-maxDocs=<num>]      Maximum number of similar documents to be retrived. Default value is 100.")
    System.err.println("\t[-encoding=<codec>]   Encoding of the input pipe file. Default value is utf-8.")
    System.err.println("\t[-fieldSep=<char>]    Character used to separate the fields of the pipe file. Default value is ´|´")
    System.err.println("\t[--hasHeader]         If present, it will skip the first line of the csv file")
    System.exit(1)
  }

  val parameters = args.foldLeft[Map[String, String]](Map()) {
    case (map, par) =>
      val split = par.split(" *= *", 2)

      split.size match {
        case 1 => map + (split(0).substring(2) -> "")
        case 2 => map + (split(0).substring(1) -> split(1))
        case _ => usage(); map
      }
  }

  if (!Set("inputPipeFile", "schema", "confFile").forall(parameters.contains)) usage()

  ConfMain.parseConfig(new File(parameters("confFile"))) match {
    case Success((finder, filters, reporters)) =>
      val schema: Map[Int, String] = parameters("schema").trim.split(" *, *").map(_.trim)
        .map(_.split(" *: *",2)).foldLeft(Map[Int,String]()) {
        case (map, elem) => map + (elem(0).trim.toInt -> elem(1).trim)
      }
      val encoding: String = parameters.getOrElse("encoding", "utf-8")
      val fieldSep: Char = parameters.getOrElse("fieldSep", "|").headOption.getOrElse('|')
      val docProducer: CSVProducer = new CSVProducer(parameters("inputPipeFile"), schema, parameters.contains("hasHeader"),
                                        fieldSeparator = fieldSep, encoding)
      val auxQuery: Option[String] = parameters.get("auxQuery")
      val maxDocs: Int = parameters.getOrElse("maxDocs", "100").toInt
      val otherFields: Seq[String] = parameters.getOrElse("otherFields", "").trim.split(" *, *").map(_.trim).toSeq
      val sd: SimilarDocs = new SimilarDocs(finder, filters, reporters, auxQuery, maxDocs, otherFields)

      docProducer.getDocuments.foreach(sd.processSimilars)

      sd.close()
    case Failure(exception) =>
      Console.err.println(s"Parsing config file ERROR: ${exception.getMessage}")
      System.exit(1)
  }
}
