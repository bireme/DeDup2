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
        .setSkipHeaderRecord(hasHeader).setIgnoreEmptyLines(true).setQuote(null)
      val reader: InputStreamReader = Source.fromFile(csvFile, csvFileEncoding).reader()
      val parser: CSVParser = CSVParser.builder().setFormat(formatBuilder.get()).setReader(reader).get()

      getDocumentsLazy(parser, schema, parser.iterator().asScala)
    } match {
      case Success(list) => list
      case Failure(exception: Throwable) =>
        exception.printStackTrace()
        LazyList.empty[Document]
    }
  }

  private def getDocumentsLazy(parser: CSVParser,
                               schema: Map[Int,String],
                               iterator: Iterator[CSVRecord]): LazyList[Document] = {
    if (iterator.hasNext) {
      Try {
        val record: CSVRecord = iterator.next()
        val recSize: Int = record.size()

        val fields: Seq[(String, String)] = schema.foldLeft(Seq[(String,String)]()) {
          case (flds, (pos, fld)) =>
            //println(s"fld=$fld pos=$pos content=${record.get(pos)}")
            assert(pos < recSize, s"Invalid position. Record size=$recSize Position=$pos. Please, compare the schema position field with cvs number of fields.")
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
