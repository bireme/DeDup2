package dd.producers

import dd.interfaces.{DocsProducer, Document}
import org.apache.commons.csv.{CSVFormat, CSVParser, CSVRecord}

import java.io.InputStreamReader
import scala.io.Source
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.{Failure, Success, Try}

class CSVProducer(csvFile: String,
                  schema: Map[Int,String],
                  hasHeader: Boolean,
                  fieldSeparator: Char =',',
                  csvFileEncoding: String = "utf-8") extends DocsProducer {
  override def getDocuments: LazyList[Document] = {
    Try {
      val formatBuilder: CSVFormat.Builder = CSVFormat.Builder.create().setDelimiter(fieldSeparator).setTrim(true)
        .setSkipHeaderRecord(hasHeader)
      val reader: InputStreamReader = Source.fromFile(csvFile, csvFileEncoding).reader()
      val parser: CSVParser = new CSVParser(reader, formatBuilder.build())

      getDocumentsLazy(parser, schema, parser.iterator().asScala)
    } match {
      case Success(list) => list
      case Failure(_) => LazyList.empty[Document]
    }
  }

  private def getDocumentsLazy(parser: CSVParser,
                               schema: Map[Int,String],
                               iterator: Iterator[CSVRecord]): LazyList[Document] = {
    if (iterator.hasNext) {
      Try {
        val record: CSVRecord = iterator.next()
        //val size = record.size()
        val fields: Seq[(String, String)] = schema.foldLeft(Seq[(String,String)]()) {
          case (flds, (pos, fld)) =>
            //println(s"fld=$fld pos=$pos content=${record.get(pos)}")
            flds :+ (fld, record.get(pos))
        }
        Document(fields)
      } match {
        case Success(doc) => doc #:: getDocumentsLazy(parser, schema, iterator)
        case Failure(exception) =>
          exception.printStackTrace()
          Console.err.println(s"CSVProducer/getDocumentsLazy/${exception.getMessage}")
          getDocumentsLazy(parser, schema, iterator)
      }
    } else {
      parser.close()
      LazyList.empty[Document]
    }
  }
}
