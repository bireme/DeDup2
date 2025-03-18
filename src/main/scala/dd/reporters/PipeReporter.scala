package dd.reporters

import dd.interfaces.{CompResult, Document, Reporter}
import org.apache.commons.csv.CSVFormat

import java.io.Writer
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

class PipeReporter(writer: Writer,
                   recordSeparator: String,
                   putHeader: Boolean,
                   minTrue: Int) extends Reporter {
  private val fieldSeparator: String = "|"
  private val formatBuilder: CSVFormat.Builder = CSVFormat.Builder.create().setRecordSeparator(recordSeparator).setTrim(true)
    .setDelimiter(fieldSeparator).setAutoFlush(true)
  private val format: CSVFormat = formatBuilder.get()
  private var first: Boolean = true

  override def writeResults(originalDoc: Document,
                            currentDoc: Document,
                            otherFields: Seq[String],
                            results: Seq[CompResult]): Try[Unit] = {
    Try {
      if (putHeader && first) {
        writeHeader(format, otherFields, results)
        first = false
      }

      if (results.isEmpty) new Exception("Empty results sequence")
      else {
        val totalTrue: Int = results.foldLeft(0) {
          case (tot, result) => if (result.isSimilar) tot + 1 else tot
        }
        if (totalTrue >= minTrue) {
          Try {
            val (oOtherFields, cOtherFields) = getOtherFields(originalDoc, currentDoc, otherFields)
            val buffer: ArrayBuffer[String] = results.foldLeft(ArrayBuffer[String]()) {
              case (arr, result) => arr ++ getResultFields(result)
            }

            if (first) first = false
            else writer.write("\n")

            writer.write(format.format(oOtherFields ++ cOtherFields ++ buffer.toArray: _*))
          } match {
            case Success(_) => ()
            case Failure(exception) => throw exception
          }
        }
      }
    }
  }

  override def close(): Try[Unit] = Try(writer.close())

  private def writeHeader(format: CSVFormat,
                          otherFields: Seq[String],
                          results: Seq[CompResult]): Either[String, Unit] = {
    val array: ArrayBuffer[String] = otherFields.foldLeft(ArrayBuffer[String]()) {
      case (buff, fld) =>
        buff += (fld.trim + "_1")
        buff += (fld.trim + "_2")
    }
    results.foreach {
      _ => array ++= Array("Comparator", "Field", "Content1", "Content2", "Similarity", "isSimilar")
    }

    Try(writer.write(format.format(array.toArray:_*))) match {
      case Success(_) => Right(())
      case Failure(exception) => Left(exception.toString)
    }
  }

  private def getOtherFields(originalDoc: Document,
                             currentDoc: Document,
                             otherFields: Seq[String]): (Array[String], Array[String]) = {
    val oArray = ArrayBuffer[String]()
    otherFields foreach {
      oField =>
        val seq: Seq[String] = originalDoc.fields.filter(_._1.equals(oField)).map(_._2)
        if (seq.nonEmpty) oArray += seq.mkString(fieldSeparator)
    }

    val cArray = ArrayBuffer[String]()
    otherFields foreach {
      oField =>
        val seq: Seq[String] = currentDoc.fields.filter(_._1.equals(oField)).map(_._2)
        if (seq.nonEmpty) cArray += seq.mkString(fieldSeparator)
    }

    (oArray.toArray, cArray.toArray)
  }

  private def getResultFields(result: CompResult): Array[String] = {
    Array[String](result.name, result.fieldName,
      result.originalFieldOther.getOrElse(result.originalField),
      result.currentFieldOther.getOrElse(result.currentField),
      result.similarity.toString, result.isSimilar.toString)
  }
}