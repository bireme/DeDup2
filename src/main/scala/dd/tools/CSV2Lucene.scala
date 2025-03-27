package dd.tools

import dd.NGAnalyzer
import dd.interfaces.DocsProducer
import dd.producers.CSVProducer

import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Success}

object CSV2Lucene extends App {
  private def usage(): Unit = {
    System.err.println("usage: CSV2Lucene <options>")
    System.err.println("options:")
    System.err.println("\t-csvFile=<path> Path to the comma separated value file (csv)")
    System.err.println("\t-index=<path> Path to the index to be created")
    System.err.println("\t-schema=(<pos>=<fieldName>,...,<pos>=<fieldName>|file=<path>) Associate the csv field position (start with 0)")
    System.err.println("\t\twith the Lucene document field name. If the parameter starts with file= then the corresponding schema file path will be used.")
    System.err.println("\t-fieldToIndex=<name> Name of the field to be indexed")
    System.err.println("\t[-fieldSeparator=<char>] Character indication the field separator. Default value is ','.")
    System.err.println("\t[-encoding=<str>] The csv character encoding. Default value is 'utf-8'")
    System.err.println("\t[--hasHeader] If present indicates the csv file has header.")

    System.exit(1)
  }

  private val parameters: Map[String, String] = args.foldLeft[Map[String, String]](Map()) {
    case (map, par) =>
      val split = par.split(" *= *", 2)
      if (split.size == 1) map + ((split(0).substring(2), ""))
      else map + ((split(0).substring(1), split(1)))
  }

  println("PArameters:")
  parameters.foreach(param => println(s"\t${param._1}=${param._2}"))
  println()

  if (!Set("csvFile", "index", "schema", "fieldToIndex").forall(parameters.contains)) usage()

  private val csvFile: String = parameters("csvFile")
  private val indexPath: String = parameters("index")
  private val schema: String = parameters("schema").trim
  private val fieldToIndex: String = parameters("fieldToIndex")
  private val fieldSeparator: Char = parameters.getOrElse("fieldSeparator", ",").head
  private val encoding: String = parameters.getOrElse("encoding", "utf-8")
  private val hasHeader: Boolean = parameters.contains("hasHeader")

  private val schemaContent: String = if (schema.startsWith("file=")) {
    val src: BufferedSource = Source.fromFile(schema.substring(5))
    val content: String = src.mkString.trim

    src.close()
    content
  } else schema
  private val schemaMap: Map[Int, String] = schemaContent.split("\\s*(,|\r?\n\r?)\\s*").map(_.split(" *= *", 2))
    .map(elem => (elem(0).toInt, elem(1))).toMap
  assert(schemaMap.values.exists(_.equals(fieldToIndex)), s"Field to index [$fieldToIndex] is not present in the schema file.")

  private val producer: DocsProducer = new CSVProducer(csvFile, schemaMap, hasHeader, fieldSeparator, encoding)

  System.out.println("Indexing ...")

  Tools.createLuceneIndex(producer, indexPath, fieldToIndex, new NGAnalyzer()) match {
    case Success(_) => System.out.println("Indexing finished successfully!")
    case Failure(exception) => exception.printStackTrace()
  }
}